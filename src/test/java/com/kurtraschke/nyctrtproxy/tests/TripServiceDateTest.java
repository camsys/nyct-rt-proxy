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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtimeNYCT;
import com.kurtraschke.nyctrtproxy.model.NyctTripId;
import com.kurtraschke.nyctrtproxy.model.Status;
import com.kurtraschke.nyctrtproxy.model.TripMatchResult;
import org.junit.Before;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.calendar.ServiceDate;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Set;

import static org.junit.Assert.*;

public class TripServiceDateTest extends LazyMatchingTest {

  @Override
  @Before
  public void before() {
    setAgencyId("MTASBWY");
    Injector injector = Guice.createInjector(getTestModule("20220224_0052_nyct_subways_gtfs_modified.zip", "MTASBWY", false));
    injector.injectMembers(this);
  }

  public TripServiceDateTest() {
    super("A", "gtfs-ace-02242022-000505.pb");
  }

  // Update now that we are checking stops to end - northbound trains are skipping stops.

  @Override
  public void checkMatchResult(long timestamp, NyctTripId rtTripId, GtfsRealtime.TripUpdateOrBuilder tripUpdate,
                               TripMatchResult result) throws Exception {
    int departureTime = rtTripId.getOriginDepartureTime();
    GtfsRealtimeNYCT.NyctTripDescriptor extension = tripUpdate.getTripOrBuilder().getExtension(GtfsRealtimeNYCT.nyctTripDescriptor);
    String trainId = extension.getTrainId();

    switch(trainId){
      case "1A 2351 HOW/207":
        assertMatch(result, "51824", "20220223", Status.LOOSE_MATCH_COERCION);
        break;
      case "1A 0021 HOW/207":
        assertNoMatch(result);
        break;
      default:
        break;
    }

  }

  private void assertMatch(TripMatchResult result, String expectedTrip, String expectedDate, Status expectedStatus) throws ParseException {
    assertTrue(result.hasResult());
    GtfsRealtime.TripDescriptor tripDescriptor = result.getTripUpdate().getTrip();
    Trip trip = result.getResult().getTrip();
    AgencyAndId serviceId = trip.getServiceId();

    String tripStartDate = tripDescriptor.getStartDate();
    String matchedTripId = trip.getId().getId();

    Date startDate = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH).parse(tripStartDate);
    ServiceDate serviceDate = new ServiceDate(startDate);

    Set<AgencyAndId> serviceIdsForServiceDate = getGtfsDataService().getServiceIdsOnDate(serviceDate);

    assertEquals(expectedTrip, matchedTripId);
    assertEquals(expectedStatus, result.getStatus());
    assertEquals(expectedDate, tripStartDate);
    assertTrue(serviceIdsForServiceDate.contains(serviceId));

  }

  private static void assertNoMatch(TripMatchResult result) {
    assertFalse(result.getStatus().isMatch());
  }
}
