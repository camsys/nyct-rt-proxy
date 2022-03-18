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

import com.google.inject.Inject;
import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtimeNYCT;
import com.kurtraschke.nyctrtproxy.model.NyctTripId;
import com.kurtraschke.nyctrtproxy.model.TripMatchResult;
import com.kurtraschke.nyctrtproxy.services.ActivatedTripMatcher;
import com.kurtraschke.nyctrtproxy.services.LazyTripMatcher;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static com.kurtraschke.nyctrtproxy.util.NycRealtimeUtil.*;

public abstract class LazyMatchingTest extends RtTestRunner {

  @Inject
  private LazyTripMatcher lazyTripMatcher;

  @Inject
  private ActivatedTripMatcher atm;

  private String routeId;
  private String filename;

  public LazyMatchingTest(String routeId, String filename) {
    this.routeId = routeId;
    this.filename = filename;
  }

  public abstract void checkMatchResult(long timestamp, NyctTripId rtid, TripUpdateOrBuilder tripUpdate, TripMatchResult result) throws Exception;

  @Test
  public void test() throws Exception {

    FeedMessage msg = readFeedMessage(filename);

    List<GtfsRealtimeNYCT.TripReplacementPeriod> trpList = msg.getHeader().getExtension(GtfsRealtimeNYCT.nyctFeedHeader)
            .getTripReplacementPeriodList();

    for (GtfsRealtimeNYCT.TripReplacementPeriod trp : trpList){
      if (routeId.equals(trp.getRouteId())) {

        List<TripUpdate> updates = msg.getEntityList().stream()
                .filter(FeedEntity::hasTripUpdate)
                .map(FeedEntity::getTripUpdate)
                .filter(tu -> tu.getTrip().getRouteId().equals(routeId))
                .collect(Collectors.toList());

        TimeRange range = trp.getReplacementPeriod();
        Date start = range.hasStart() ? new Date(range.getStart() * 1000) : earliestTripStart(updates);
        Date end = range.hasEnd() ? new Date(range.getEnd() * 1000) : new Date(msg.getHeader().getTimestamp() * 1000);

        atm.initForFeed(start, end, Collections.singleton(routeId));

        long timestamp = msg.getHeader().getTimestamp();

        for (TripUpdate tu : updates) {
          TripUpdate.Builder tripUpdateBuilder = TripUpdate.newBuilder(tu);

          TripDescriptor.Builder tripBuilder = tripUpdateBuilder.getTripBuilder();
          NyctTripId rtTripId = NyctTripId.buildFromTripDescriptor(tripBuilder);

          if (rtTripId != null) {
            tripUpdateBuilder.getStopTimeUpdateBuilderList().forEach(stub -> {
              if (!(stub.getStopId().endsWith("N") || stub.getStopId().endsWith("S"))) {
                stub.setStopId(stub.getStopId() + rtTripId.getDirection());
              }
            });

            tripBuilder.setTripId(rtTripId.toString());
          }

          if (tripBuilder.getStartDate().length() > 8) {
            tripBuilder.setStartDate(fixedStartDate(tripBuilder));
          }

          TripMatchResult tripMatchResult = lazyTripMatcher.match(tripUpdateBuilder, rtTripId, timestamp);

          checkMatchResult(timestamp, rtTripId, tripUpdateBuilder, tripMatchResult);
        }
      }
    }
  }

}
