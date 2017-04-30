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
package com.kurtraschke.nyctrtproxy.services;

import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.calendar.CalendarServiceData;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.onebusaway.gtfs.services.GtfsRelationalDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.kurtraschke.nyctrtproxy.model.ActivatedTrip;
import com.vividsolutions.jts.index.strtree.SIRtree;

import java.util.Date;
import java.util.IntSummaryStatistics;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import com.google.inject.Inject;
import javax.inject.Named;

/**
 * Find currently-active trips for a given time. (Only needed for ActivatedTripMatcher)
 *
 * @author kurt
 */
public class TripActivator {

  private CalendarServiceData _csd;

  private GtfsRelationalDao _dao;

  private SIRtree tripTimesTree;

  private int maxLookback;
  
  private String _agencyId = "MTA NYCT";
  
  private static final Logger _log = LoggerFactory.getLogger(TripActivator.class);

  @Inject
  public void setCalendarServiceData(CalendarServiceData csd) {
    _csd = csd;
  }
  
  @Inject(optional = true)
  public void setAgencyMatchId(@Named("NYCT.gtfsAgency") String agencyid) {
	  _agencyId = agencyid;
	  _log.info("Using AgencyId "+_agencyId);
  }

  @Inject
  public void setGtfsRelationalDao(GtfsRelationalDao dao) {
    _dao = dao;
  }

  @PostConstruct
  public void start() {
    tripTimesTree = new SIRtree();

    _dao.getAllTrips().forEach(trip -> {
      IntSummaryStatistics iss = _dao.getStopTimesForTrip(trip)
              .stream()
              .flatMapToInt(st -> {
                IntStream.Builder sb = IntStream.builder();

                if (st.isArrivalTimeSet()) {
                  sb.add(st.getArrivalTime());
                }

                if (st.isDepartureTimeSet()) {
                  sb.add(st.getDepartureTime());
                }

                return sb.build();
              }).summaryStatistics();

      tripTimesTree.insert(iss.getMin(), iss.getMax(), trip);
    });
    tripTimesTree.build();

    maxLookback = getMaxLookback();
  }

  private int getMaxLookback() {
    return (int) Math.ceil(_dao.getAllStopTimes().stream()
            .flatMapToInt(st -> {
              IntStream.Builder sb = IntStream.builder();

              if (st.isArrivalTimeSet()) {
                sb.add(st.getArrivalTime());
              }

              if (st.isDepartureTimeSet()) {
                sb.add(st.getDepartureTime());
              }

              return sb.build();
            })
            .max()
            .getAsInt() / 86400.0);
  }

  public Stream<ActivatedTrip> getTripsForRangeAndRoutes(Date start, Date end, Set<String> routeIds) {
    ServiceDate startDate = new ServiceDate(start);

    return Stream.iterate(startDate, ServiceDate::previous)
            .limit(maxLookback)
            .flatMap(sd -> {
              Set<AgencyAndId> serviceIdsForDate = _csd.getServiceIdsForDate(sd);

              int sdOrigin = (int) (sd.getAsCalendar(_csd.getTimeZoneForAgencyId(_agencyId)).getTimeInMillis() / 1000);

              int startTime = (int) ((start.getTime() / 1000) - sdOrigin);
              int endTime = (int) ((end.getTime() / 1000) - sdOrigin);

              Stream<Trip> tripsStream = tripTimesTree.query(startTime, endTime).stream()
                      .map(Trip.class::cast);

              return tripsStream
                      .filter(t -> routeIds.contains(t.getRoute().getId().getId()))
                      .filter(t -> serviceIdsForDate.contains(t.getServiceId()))
                      .map(t -> new ActivatedTrip(sd, t, _dao.getStopTimesForTrip(t)));
            });

  }

  public Stream<ActivatedTrip> getTripsForRangeAndRoute(Date start, Date end, String routeId) {
    return getTripsForRangeAndRoutes(start, end, ImmutableSet.of(routeId));
  }

}
