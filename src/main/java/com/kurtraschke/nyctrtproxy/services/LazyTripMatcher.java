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
package com.kurtraschke.nyctrtproxy.services;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.transit.realtime.GtfsRealtime;
import com.kurtraschke.nyctrtproxy.model.ActivatedTrip;
import com.kurtraschke.nyctrtproxy.model.NyctTripId;
import com.kurtraschke.nyctrtproxy.model.Status;
import com.kurtraschke.nyctrtproxy.model.TripMatchResult;
import org.onebusaway.gtfs.impl.calendar.CalendarServiceDataFactoryImpl;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Route;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.calendar.CalendarServiceData;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.onebusaway.gtfs.services.GtfsRelationalDao;
import org.onebusaway.gtfs.services.calendar.CalendarServiceDataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of trip matching that allows a looser matching.
 *
 * Trips can be coerced, ie the RT origin-departure time is slightly later than the matched static time,
 * or they can be for a different service day. We do not test that the stops in the RT and static trip
 * match, since results of this match may need to be merged.
 *
 * @author Simon Jacobs
 */
public class LazyTripMatcher implements TripMatcher {

  private int _lateTripLimitSec = 3600; // 1 hour
  private String _agencyId = "MTA NYCT";
  private GtfsRelationalDao _dao;
  private CalendarServiceData _csd;
  private boolean _looseMatchDisabled = false;
  private boolean _allowServiceDayCoercion = true;

  private static final Logger _log = LoggerFactory.getLogger(LazyTripMatcher.class);

  private Set<String> _mergableRoutes = Sets.newHashSet("D");

  @Inject
  public void setGtfsRelationalDao(GtfsRelationalDao dao) {
    _dao = dao;
  }

  @Inject
  public void setCalendarServiceData(CalendarServiceData csd) {
    _csd = csd;
  }
  
  @Inject(optional = true)
  public void setAgencyMatchId(@Named("NYCT.gtfsAgency") String agencyid) {
	  _agencyId = agencyid;
	  _log.info("Using AgencyId "+_agencyId);
  }

  @Inject(optional = true)
  public void setLateTripLimitSec(@Named("NYCT.lateTripLimitSec") int lateTripLimitSec) {
    _lateTripLimitSec = lateTripLimitSec;
  }

  @Inject(optional = true)
  public void setAllowServiceDayCoercion(boolean allowServiceDayCoercion) {
    _allowServiceDayCoercion = allowServiceDayCoercion;
  }

  public void setLooseMatchDisabled(boolean looseMatchDisabled) {
    _looseMatchDisabled = looseMatchDisabled;
  }

  public void setMergableRoutes(String routesStr) {
      _mergableRoutes = new HashSet<>(Arrays.asList(routesStr.split(",")));
  }

  @Override
  public TripMatchResult match(GtfsRealtime.TripUpdateOrBuilder tu, NyctTripId id, long timestamp) {
    if (id == null)
      return new TripMatchResult(tu, Status.BAD_TRIP_ID);

    ServiceDate sd = new ServiceDate(new Date(timestamp * 1000));
    Set<TripMatchResult> candidates = Sets.newHashSet();
    boolean foundTripWithStartTime = addCandidates(tu, id, sd, candidates);

    // Look back to previous day. Static IDs have a 26-hour service period, RT IDs are relative to midnight.
    // Latest trip departure is 26:02:00, so this technically allows us to consider trips that are up to 58min late (and most likely later.)
    if (id.getOriginDepartureTime() < 3 * 60 * 100)
      foundTripWithStartTime |= addCandidates(tu, id.relativeToPreviousDay(), sd.previous(), candidates);

    if (candidates.isEmpty())
      return new TripMatchResult(tu, foundTripWithStartTime ? Status.NO_MATCH : Status.NO_TRIP_WITH_START_DATE);
    else
      return Collections.max(candidates); // get BEST match. see TripMatchResult::compareTo
  }

  @Override
  public void initForFeed(Date start, Date end, Set<String> routeIds) {
    // do nothing
  }

  // Find possible match candidates among static trips.
  // return true if trips were found with start date
  private boolean addCandidates(GtfsRealtime.TripUpdateOrBuilder tu, NyctTripId id, ServiceDate sd, Set<TripMatchResult> candidates) {

    boolean found = false;
    Route r = _dao.getRouteForId(new AgencyAndId(_agencyId, tu.getTrip().getRouteId()));
    Set<AgencyAndId> serviceIds = _csd.getServiceIdsForDate(sd);

    // We check through all trips. This could be easily restricted, but performance has not been a problem.
    for (Trip trip : _dao.getTripsForRoute(r)) {
      List<StopTime> stopTimes = _dao.getStopTimesForTrip(trip);
      if (stopTimes.isEmpty())
        continue;
      NyctTripId atid = NyctTripId.buildFromGtfs(trip, stopTimes);
      if (atid == null || !atid.routeDirMatch(id))
        continue;
      int start = stopTimes.get(0).getDepartureTime(); // in sec into day.
      int end = stopTimes.get(stopTimes.size()-1).getArrivalTime();
      boolean onServiceDay = serviceIds.contains(trip.getServiceId());
      if (!onServiceDay && !_allowServiceDayCoercion) {
        continue;
      }
      if (atid.strictMatch(id) && onServiceDay) {
        found = true;
        candidates.add(new TripMatchResult(tu, new ActivatedTrip(sd, trip, stopTimes)));
        continue;
      }
      // loose match, RT trip could be late relative to static trip
      int delta = (int) (((double) id.getOriginDepartureTime())*0.6 - start);
      if (!_looseMatchDisabled && delta >= 0 && delta < _lateTripLimitSec) {
        found &= onServiceDay;

        ActivatedTrip at = new ActivatedTrip(sd, trip, stopTimes);
        TripMatchResult result = TripMatchResult.looseMatch(tu, at, delta, onServiceDay);
        // disable trips that are coerced AND on a different day
        if ((onServiceDay || delta == 0) && (result.stopsMatchToEnd() || _mergableRoutes.contains(tu.getTrip().getRouteId())))
          candidates.add(result);

      }
    }

    return found;
  }

}
