/*
 * Copyright (C) 2015 Kurt Raschke <kurt@kurtraschke.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.kurtraschke.nyctrtproxy;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.kurtraschke.nyctrtproxy.model.MatchMetrics;
import com.kurtraschke.nyctrtproxy.services.ProxyDataListener;
import com.kurtraschke.nyctrtproxy.services.TripUpdateProcessor;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeFullUpdate;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeGuiceBindingTypes.TripUpdates;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeSink;

import com.google.protobuf.ExtensionRegistry;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtimeNYCT;

import org.apache.http.conn.HttpClientConnectionManager;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Named;

/**
 *
 * @author kurt
 */
public class ProxyProvider {

  private static final org.slf4j.Logger _log = LoggerFactory.getLogger(ProxyProvider.class);

  private FeedManager _feedManager;

  private static final ExtensionRegistry _extensionRegistry;

  private GtfsRealtimeSink _tripUpdatesSink;

  private ScheduledExecutorService _scheduledExecutorService;

  private ScheduledFuture _updater;

  private TripUpdateProcessor _processor;

  private ProxyDataListener _listener;

  private int _nTries = 5;

  private int _refreshRate = 60;

  private int _retryDelay = 5;


  private List<Integer> _feedIds = Arrays.asList(1, 2, 11, 16, 21);

  static {
    _extensionRegistry = ExtensionRegistry.newInstance();
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctFeedHeader);
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctTripDescriptor);
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctStopTimeUpdate);
  }

  @Inject
  public void setFeedManager(FeedManager feedManager) {
    _feedManager = feedManager;
  }

  @Inject
  public void setTripUpdatesSink(@TripUpdates GtfsRealtimeSink tripUpdatesSink) {
    _tripUpdatesSink = tripUpdatesSink;
  }

  @Inject
  public void setHttpClientConnectionManager(HttpClientConnectionManager connectionManager) {
    _feedManager.setHttpClientConnectionManager(connectionManager);
  }

  @Inject
  public void setScheduledExecutorService(ScheduledExecutorService service) {
    _scheduledExecutorService = service;
  }

  @Inject(optional = true)
  public void setNTries(@Named("NYCT.nTries") int nTries) {
    _nTries = nTries;
  }

  @Inject(optional = true)
  public void setFeedIds(@Named("NYCT.feedIds") String json) {
    Type type = new TypeToken<List<Integer>>(){}.getType();
    _feedIds = new Gson().fromJson(json, type);
  }

  @Inject(optional = true)
  public void setRefreshRate(@Named("NYCT.refreshRate") int refreshRate) {
    _refreshRate = refreshRate;
  }

  @Inject(optional = true)
  public void setRetryDelay(@Named("NYCT.retryDelay") int retryDelay) {
    _retryDelay = retryDelay;
  }

  @Inject
  public void setTripUpdateProcessor(TripUpdateProcessor processor) {
    _processor = processor;
  }

  @Inject(optional = true)
  public void setListener(ProxyDataListener listener) {
    _listener = listener;
  }

  @PostConstruct
  public void start() {
    if (_scheduledExecutorService != null)
      _updater = _scheduledExecutorService.scheduleWithFixedDelay(this::update, 0, _refreshRate, TimeUnit.SECONDS);
  }

  @PreDestroy
  public void stop() {
    if (_scheduledExecutorService != null) {
      _updater.cancel(false);
      _scheduledExecutorService.shutdown();
    }
  }

  public void update() {

    GtfsRealtimeFullUpdate grfu = new GtfsRealtimeFullUpdate();

    List<TripUpdate> tripUpdates = Lists.newArrayList();

    MatchMetrics totalMetrics = new MatchMetrics();

    // For each feed ID, read in GTFS-RT, process trip updates, push to output.
    for (int feedId : _feedIds) {
      String feedUrl = _feedManager.getFeedOrDefault(String.valueOf(feedId));
      FeedMessage message = null;
      for (int tries = 0; tries < _nTries; tries++) {
        try {

          try (InputStream streamContent = _feedManager.getStream(feedUrl, String.valueOf(feedId))) {
           message = FeedMessage.parseFrom(streamContent, _extensionRegistry);
           if (!message.getEntityList().isEmpty())
            break;
           Thread.sleep(_retryDelay * 1000);
          }
        } catch (Exception e) {
          _log.debug("broke", e);
          _log.error("Error parsing protocol buffer for feed={}. try={}, retry={}. Error={}",
                  feedId, tries, tries < _nTries, e.getMessage());
        }
      }

      if (message != null) {
        try {
          tripUpdates.addAll(_processor.processFeed(feedId, message, totalMetrics));
          _listener.sendMetrics(_processor.getCloudwatchNamespace());
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    for (TripUpdate tu : tripUpdates) {
      FeedEntity.Builder feb = FeedEntity.newBuilder();
      feb.setTripUpdate(tu);
      feb.setId(tu.getTrip().getTripId());
      grfu.addEntity(feb.build());
    }

    _log.info("writing {} total trip updates", tripUpdates.size());

    _tripUpdatesSink.handleFullUpdate(grfu);

    if (_listener != null) {
      _listener.prepareMatchesTotal(totalMetrics, _processor.getCloudwatchNamespace());
      _listener.sendMetrics(_processor.getCloudwatchNamespace());
    }
  }


}
