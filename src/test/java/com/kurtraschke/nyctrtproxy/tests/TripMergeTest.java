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
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtimeNYCT;
import com.kurtraschke.nyctrtproxy.model.MatchMetrics;
import com.kurtraschke.nyctrtproxy.model.NyctTrainId;
import com.kurtraschke.nyctrtproxy.model.NyctTripId;
import com.kurtraschke.nyctrtproxy.model.TripMatchResult;
import com.kurtraschke.nyctrtproxy.services.TripUpdateProcessor;
import org.junit.Test;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.services.GtfsDataService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TripMergeTest extends RtTestRunner {

  @Inject
  private TripUpdateProcessor _processor;

  @Inject
  private GtfsDataService _dao;

  @Test
  public void testMerging() throws Exception {
    String protobuf = "21_2017-03-13.pb";
    int feedId = 21;

    FeedMessage msg = readFeedMessage(protobuf);
    List<TripUpdate> updates = _processor.processFeed(feedId, msg, new MatchMetrics());

    // we expect these to get merged:
    // 098650_D..S (D01, D03) 099050_D..S (lots of stops)
    assertTrue(getByRouteDirectionAndTime(updates, "D", "S", 99050).isEmpty());

    List<TripUpdate> tus = getByRouteDirectionAndTime(updates, "D", "S", 98650);
    assertEquals(1, tus.size());

    TripUpdate update = tus.get(0);

    List<String> stopTimes = update.getStopTimeUpdateList().stream().map(stu -> stu.getStopId()).collect(Collectors.toList());
    List<String> gtfsStops = getStopTimesForTripUpdate(update).stream().map(s -> s.getStop().getId().getId()).collect(Collectors.toList());

    assertEquals(stopTimes, gtfsStops);

    List<TripUpdate.StopTimeUpdate> sortedStus = new ArrayList<>(update.getStopTimeUpdateList());
    Collections.sort(sortedStus, (s, t) -> (int) (s.getDeparture().getTime() - t.getDeparture().getTime()));
    List<String> sortedStopTimes = sortedStus.stream().map(stu -> stu.getStopId()).collect(Collectors.toList());
    assertEquals(stopTimes, sortedStopTimes);
  }

  private List<StopTime> getStopTimesForTripUpdate(TripUpdate tu) {
    String tripId = tu.getTrip().getTripId();
    Trip trip = _dao.getTripForId(new AgencyAndId("MTA NYCT", tripId));
    if (trip == null)
      return Collections.emptyList();
    return _dao.getStopTimesForTrip(trip);
  }

  private List<TripUpdate> getByRouteDirectionAndTime(List<TripUpdate> updates, String routeId, String direction, int odtime) {
    return updates.stream()
            .filter(tu -> {
              NyctTripId id = NyctTripId.buildFromTripDescriptor(tu.getTrip());
              return id.getRouteId().equals(routeId) && id.getDirection().equals(direction) && id.getOriginDepartureTime() == odtime;
            }).collect(Collectors.toList());
  }

  @Test
  public void mergedResultTest(){
    String tripId1 = "098650_D..S";
    String trainId1 = "1D 1102 STL/205";
    long arrivalTime1 = 1645051560000l; // Wednesday, February 16, 2022 5:46:00 PM GMT-05:00
    long departureTime1 = 1645051620000l; //Wednesday, February 16, 2022 5:47:00 PM GMT-05:00

    List<ArrivalDeparturePair> arrivalDeparturePairs1 = new ArrayList<>(1);
    arrivalDeparturePairs1.add(new ArrivalDeparturePair(arrivalTime1, departureTime1));

    TripMatchResult tripMatchResult1 = getTripMatchResult(tripId1, trainId1, arrivalDeparturePairs1);

    String tripId2 =  "099050_D..S";
    String trainId2 = "1D 1105 205/TST";
    long arrivalTime2 = 1645051620000l; //Wednesday, February 16, 2022 5:47:00 PM GMT-05:00
    long departureTime2 = 1645051680000l; //Wednesday, February 16, 2022 5:48:00 PM GMT-05:00
    long arrivalTime3 = 1645051740000l; // Wednesday, February 16, 2022 5:49:00 PM GMT-05:00
    long departureTime3 = 1645051800000l; // Wednesday, February 16, 2022 5:50:00 PM GMT-05:00

    List<ArrivalDeparturePair> arrivalDeparturePairs2 = new ArrayList<>(2);
    arrivalDeparturePairs2.add(new ArrivalDeparturePair(arrivalTime2, departureTime2));
    arrivalDeparturePairs2.add(new ArrivalDeparturePair(arrivalTime3, departureTime3));

    TripMatchResult tripMatchResult2 = getTripMatchResult(tripId2, trainId2, arrivalDeparturePairs2);

    TripUpdateProcessor tripUpdateProcessor = new TripUpdateProcessor();
    TripMatchResult result = tripUpdateProcessor.mergedResult(tripMatchResult1, tripMatchResult2);

    assertNotNull(result);

    List<ArrivalDeparturePair> arrivalDeparturePairs = new ArrayList<>();

    result = tripUpdateProcessor.mergedResult(getTripMatchResult(tripId1, trainId1, arrivalDeparturePairs), tripMatchResult2);
    assertNull(result);

    result = tripUpdateProcessor.mergedResult(tripMatchResult1, getTripMatchResult(tripId2, trainId2, arrivalDeparturePairs));
    assertNull(result);

  }



  private TripMatchResult getTripMatchResult(String tripId, String trainId, List<ArrivalDeparturePair> arrivalDeparturePairs) {
    GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder();

    for (ArrivalDeparturePair adPair : arrivalDeparturePairs){
      GtfsRealtime.TripUpdate.StopTimeEvent.Builder arrivalTimeBuilder = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
      arrivalTimeBuilder.setTime(adPair.getArrivalTime());

      GtfsRealtime.TripUpdate.StopTimeEvent.Builder departureTimeBuilder = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
      departureTimeBuilder.setTime(adPair.getDepartureTime());

      GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
      stopTimeUpdateBuilder.setArrival(arrivalTimeBuilder.build());
      stopTimeUpdateBuilder.setDeparture(departureTimeBuilder.build());
      TripUpdate.StopTimeUpdate stopTimeUpdate = stopTimeUpdateBuilder.build();

      tripUpdateBuilder.addStopTimeUpdate(stopTimeUpdate);

    }

    GtfsRealtime.TripDescriptor.Builder tripDescriptorBuilder = GtfsRealtime.TripDescriptor.newBuilder();
    tripDescriptorBuilder.setTripId(tripId)
            .setExtension(GtfsRealtimeNYCT.nyctTripDescriptor,
                    GtfsRealtimeNYCT.NyctTripDescriptor.newBuilder().setTrainId(trainId).build());
    TripDescriptor tripDescriptor = tripDescriptorBuilder.build();

    tripUpdateBuilder.setTrip(tripDescriptor);

    TripUpdate tripUpdate = tripUpdateBuilder.build();

    return new TripMatchResult(tripUpdate, null, null, 0);
  }

   private class ArrivalDeparturePair {
    long arrivalTime;
    long departureTime;
    public ArrivalDeparturePair(long arrivalTime, long departurTime){
      this.arrivalTime = arrivalTime;
      this.departureTime = departurTime;
    }

     public long getArrivalTime() {
       return arrivalTime;
     }

     public long getDepartureTime(){
      return departureTime;
     }
   }

}
