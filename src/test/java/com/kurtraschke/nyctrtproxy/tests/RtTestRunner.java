/*
 * Copyright (C) 2017 Cambridge Systematics, Inc.
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
package com.kurtraschke.nyctrtproxy.tests;

import com.google.inject.Module;
import com.google.inject.*;
import com.google.inject.name.Names;
import com.google.protobuf.ExtensionRegistry;
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtimeNYCT;
import com.kurtraschke.nyctrtproxy.services.*;
import junit.framework.TestCase;
import org.junit.Before;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.services.GtfsDataService;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public abstract class RtTestRunner {

  @Inject
  private GtfsDataService _dao;

  protected static ExtensionRegistry _extensionRegistry;
  protected String _agencyId = "MTA NYCT";

  private static Injector _injector;

  static {
    _injector = Guice.createInjector(getTestModule());

    _extensionRegistry = ExtensionRegistry.newInstance();
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctFeedHeader);
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctTripDescriptor);
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctStopTimeUpdate);
  }

  @Before
  public void before() {
    _injector.injectMembers(this);
  }

  public GtfsRealtime.FeedMessage readFeedMessage(String file) throws IOException {
    InputStream stream = this.getClass().getResourceAsStream("/" + file);
    GtfsRealtime.FeedMessage msg = GtfsRealtime.FeedMessage.parseFrom(stream, _extensionRegistry);
    return msg;
  }

  public Trip getTrip(GtfsRealtime.TripUpdate tu) {
    String tid = tu.getTrip().getTripId();
    return _dao.getTripForId(new AgencyAndId(_agencyId, tid));
  }

  protected static Module getTestModule() {
    return getTestModule("google_transit.zip", "MTA NYCT", false);
  }

  protected static Module getTestModule(String gtfsPath, String agencyId, boolean cancelUnmatchedTrips) {
    return new AbstractModule() {
      @Override protected void configure() {
        bind(File.class)
                .annotatedWith(Names.named("NYCT.gtfsPath"))
                .toInstance(getTestResource(gtfsPath));

        bind(GtfsDataService.class)
                .toProvider(GtfsDataServiceProvider.class)
                .in(Scopes.SINGLETON);

        CloudwatchProxyDataListener listener = new CloudwatchProxyDataListener();
        bind(ProxyDataListener.class)
                .toInstance(listener);

        LazyTripMatcher ltm = new LazyTripMatcher();
        ltm.setAgencyMatchId(agencyId);
        bind(TripMatcher.class)
                .toInstance(ltm);

        TripUpdateProcessor processor = new TripUpdateProcessor();
        processor.setLatencyLimit(-1);
        processor.setCancelUnmatchedTrips(cancelUnmatchedTrips);
        bind(TripUpdateProcessor.class)
                .toInstance(processor);

        /// needed for LazyMatchingTestCompareActivated:

        TripActivator ta = new TripActivator();
        ta.setAgencyMatchId(agencyId);
        bind(TripActivator.class)
                .toInstance(ta);

        bind(ActivatedTripMatcher.class)
                .toInstance(new ActivatedTripMatcher());
      }
    };
  }

  private static File getTestResource(String resourcePath) {
    try {
      return new File(TestCase.class.getResource("/" + resourcePath).toURI().getPath());
    } catch (URISyntaxException e) {
      return null;
    }
  }

  protected void setAgencyId(String agencyId) {
    _agencyId = agencyId;
  }
}
