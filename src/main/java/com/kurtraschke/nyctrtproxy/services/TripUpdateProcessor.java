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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import com.google.transit.realtime.GtfsRealtimeNYCT;
import com.google.transit.realtime.GtfsRealtimeOneBusAway;
import com.kurtraschke.nyctrtproxy.model.*;
import com.kurtraschke.nyctrtproxy.transform.StopFilterStrategy;
import com.kurtraschke.nyctrtproxy.transform.StopIdTransformStrategy;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.kurtraschke.nyctrtproxy.util.NycRealtimeUtil.earliestTripStart;
import static com.kurtraschke.nyctrtproxy.util.NycRealtimeUtil.fixedStartDate;

public class TripUpdateProcessor {

  private static final Logger _log = LoggerFactory.getLogger(TripUpdateProcessor.class);

  private Map<Integer, Set<String>> _routeBlacklistByFeed = ImmutableMap.of(1, ImmutableSet.of("D", "N", "Q"));

  private Map<Integer, Map<String, String>> _realtimeToStaticRouteMapByFeed = ImmutableMap.of(1,
          ImmutableMap.of("S", "GS", "5X", "5"),
          11, ImmutableMap.of("SS", "SI"));

  private Map<String, String> _addToTripReplacementPeriodByRoute = ImmutableMap.of("6", "6X", "7", "7X", "F", "FX");

  private Set<String> _routesWithReverseRTDirections = Collections.emptySet();

  private StopIdTransformStrategy _stopIdTransformStrategy = null;

  private int _latencyLimit = 300;

  private ProxyDataListener _listener;

  private TripMatcher _tripMatcher;

  private TripActivator _tripActivator;

  private StopFilterStrategy _stopFilterStrategy;

  private boolean _cancelUnmatchedTrips = true;

  private DirectionsService _directionsService;

  private boolean _allowDuplicates = false;

  private String _cloudwatchNamespace = null;

  // config
  @Inject(optional = true)
  public void setLatencyLimit(@Named("NYCT.latencyLimit") int limit) {
    _latencyLimit = limit;
  }

  @Inject(optional = true)
  public void setRouteBlacklistByFeed(@Named("NYCT.routeBlacklistByFeed") String json) {
    Type type = new TypeToken<Map<Integer,Set<String>>>(){}.getType();
    _routeBlacklistByFeed = new Gson().fromJson(json, type);
  }

  @Inject(optional = true)
  public void setRealtimeToStaticRouteMapByFeed(@Named("NYCT.realtimeToStaticRouteMapByFeed") String json) {
    Type type = new TypeToken<Map<Integer, Map<String, String>>>(){}.getType();
    _realtimeToStaticRouteMapByFeed = new Gson().fromJson(json, type);
  }

  @Inject(optional = true)
  public void setAddToTripReplacementPeriodByRoute(@Named("NYCT.addToTripReplacementPeriodByRoute") String json) {
    Type type = new TypeToken<Map<String, String>>(){}.getType();
    _addToTripReplacementPeriodByRoute = new Gson().fromJson(json, type);
  }

  @Inject(optional = true)
  public void setRoutesWithReverseRTDirections(@Named("NYCT.routesWithReverseRTDirections") String json) {
    Type type = new TypeToken<Set<String>>(){}.getType();
    _routesWithReverseRTDirections = new Gson().fromJson(json, type);
  }

  @Inject(optional = true)
  public void setCloudwatchNamespace(@Named("cloudwatch.namespace") String namespace) {
    _cloudwatchNamespace = namespace;
  }

    public String getCloudwatchNamespace() {
        return _cloudwatchNamespace;
    }

  @Inject(optional = true)
  public void setAllowDuplicates(boolean allowDuplicates) {
    _allowDuplicates = allowDuplicates;
  }

  @Inject(optional = true)
  public void setListener(ProxyDataListener listener) {
    _listener = listener;
  }

  @Inject(optional = true)
  public void setStopIdTransformStrategy(StopIdTransformStrategy stopIdTransformStrategy) {
    _stopIdTransformStrategy = stopIdTransformStrategy;
  }

  @Inject
  public void setTripActivator(TripActivator tripActivator) {
    _tripActivator = tripActivator;
  }

  @Inject
  public void setTripMatcher(TripMatcher tm) {
    _tripMatcher = tm;
  }

  @Inject(optional =  true)
  public void setStopFilterStrategy(StopFilterStrategy stopFilterStrategy) {
    _stopFilterStrategy = stopFilterStrategy;
  }

  @Inject(optional = true)
  public void setCancelUnmatchedTrips(boolean cancelUnmatchedTrips) {
    _cancelUnmatchedTrips = cancelUnmatchedTrips;
  }

  @Inject(optional = true)
  public void setDirectionsService(DirectionsService directionsService){
    _directionsService = directionsService;
  }

  public List<GtfsRealtime.TripUpdate> processFeed(Integer feedId, GtfsRealtime.FeedMessage fm, MatchMetrics totalMetrics) {

    long timestamp = fm.getHeader().getTimestamp();

    MatchMetrics feedMetrics = new MatchMetrics();
    feedMetrics.reportLatency(timestamp);

    if (_latencyLimit > 0 && feedMetrics.getLatency() > _latencyLimit) {
      _log.info("Feed {} ignored, too high latency = {}", feedId, feedMetrics.getLatency());
      if (_listener != null)
        _listener.reportMatchesForSubwayFeed(feedId.toString(), feedMetrics, _cloudwatchNamespace);
      return Collections.emptyList();
    }

    final Map<String, String> realtimeToStaticRouteMap = _realtimeToStaticRouteMapByFeed
            .getOrDefault(feedId, Collections.emptyMap());

    int nExpiredTus = 0, nTotalRecords = 0;

    // Read in trip updates per route. Skip trip updates that have too stale of data.
    Multimap<String, GtfsRealtime.TripUpdate> tripUpdatesByRoute = ArrayListMultimap.create();
    for (GtfsRealtime.FeedEntity entity : fm.getEntityList()) {
      if (entity.hasTripUpdate()) {
        GtfsRealtime.TripUpdate tu = entity.getTripUpdate();
        if (expiredTripUpdate(tu, fm.getHeader().getTimestamp())) {
          nExpiredTus++;
        }
        else {
          String routeId = tu.getTrip().getRouteId();
          routeId = realtimeToStaticRouteMap.getOrDefault(routeId, routeId);
          tripUpdatesByRoute.put(routeId, tu);
        }
        nTotalRecords++;
      }
    }
    reportRecordsIn(nTotalRecords, nExpiredTus, totalMetrics, feedMetrics);

    List<GtfsRealtime.TripUpdate> ret = Lists.newArrayList();

    for (GtfsRealtimeNYCT.TripReplacementPeriod trp : fm.getHeader()
            .getExtension(GtfsRealtimeNYCT.nyctFeedHeader)
            .getTripReplacementPeriodList()) {
      if (_routeBlacklistByFeed.getOrDefault(feedId, Collections.emptySet()).contains(trp.getRouteId()))
        continue;
      GtfsRealtime.TimeRange range = trp.getReplacementPeriod();

      Date start = range.hasStart() ? new Date(range.getStart() * 1000) : earliestTripStart(tripUpdatesByRoute.values());
      Date end = range.hasEnd() ? new Date(range.getEnd() * 1000) : new Date(fm.getHeader().getTimestamp() * 1000);

      // All route IDs in this trip replacement period
      Set<String> routeIds = Arrays.stream(trp.getRouteId().split(", ?"))
              .map(routeId -> realtimeToStaticRouteMap.getOrDefault(routeId, routeId))
              .collect(Collectors.toSet());

      for (String routeId : routeIds) {
        String newRouteId = _addToTripReplacementPeriodByRoute.get(routeId);
        if (newRouteId != null)
          routeIds.add(newRouteId);
      }

      // Kurt's trip matching algorithm (ActivatedTripMatcher) requires calculating currently-active static trips at this point.
      _tripMatcher.initForFeed(start, end, routeIds);

      for (String routeId : routeIds) {

        MatchMetrics routeMetrics = new MatchMetrics();

        Multimap<String, TripMatchResult> matchesByTrip = ArrayListMultimap.create();
        Collection<GtfsRealtime.TripUpdate> tripUpdates = tripUpdatesByRoute.get(routeId);
        routeMetrics.reportRecordsIn(tripUpdates.size());
        for (GtfsRealtime.TripUpdate tu : tripUpdates) {
          GtfsRealtime.TripUpdate.Builder tub = GtfsRealtime.TripUpdate.newBuilder(tu);
          GtfsRealtime.TripDescriptor.Builder tb = tub.getTripBuilder();

          // rewrite route ID for some routes
          tb.setRouteId(realtimeToStaticRouteMap.getOrDefault(tb.getRouteId(), tb.getRouteId()));

          // remove timepoints not in GTFS... in some cases this means there may be no STUs left (ex. H shuttle at H19S.)
          removeTimepoints(tub);

          // get ID which consists of route, direction, origin-departure time, possibly a path identifier (for feed 1.)
          NyctTripId rtid = NyctTripId.buildFromTripDescriptor(tb, _routesWithReverseRTDirections);

          // If we were able to parse the trip ID, there are various fixes
          // we may need to apply.
          if (rtid != null) {

            // Fix stop IDs which don't include direction
            tub.getStopTimeUpdateBuilderList().forEach(stub -> {
              if (!(stub.getStopId().endsWith("N") || stub.getStopId().endsWith("S"))) {
                stub.setStopId(stub.getStopId() + rtid.getDirection());
              } else if (_routesWithReverseRTDirections.contains(tb.getRouteId())) {
                String stopId = stub.getStopId();
                stub.setStopId(stopId.substring(0, stopId.length() - 1) + rtid.getDirection());
              }
              if (_stopIdTransformStrategy != null) {
                String stopId = stub.getStopId();
                stopId = _stopIdTransformStrategy.transform(rtid.getRouteId(), rtid.getDirection(), stopId);
                stub.setStopId(stopId);
              }
            });

            // Re-set the trip ID to the parsed trip ID; coerces IDs to a uniform format.
            // If the trip is matched, the ID will be rewritten again to the corresponding static trip ID below.
            tb.setTripId(rtid.toString());
          } else {
            _log.error("invalid trip_id={} train_id={}", tb.getTripId(), tb.getExtension(GtfsRealtimeNYCT.nyctTripDescriptor)
                    .getTrainId());
          }

          // Some routes have start date set incorrectly
          if (tb.getStartDate().length() > 8) {
            tb.setStartDate(fixedStartDate(tb));
          }

          TripMatchResult result = _tripMatcher.match(tub, rtid, fm.getHeader().getTimestamp());
          matchesByTrip.put(result.getTripId(), result);
        }

        // For TUs that match to same trip - possible they should be merged (route D has mid-line relief points where trip ID changes)
        // If they are NOT merged, then drop the matches for the worse ones
        for (Collection<TripMatchResult> matches : matchesByTrip.asMap().values()) {
          if (!tryMergeResult(matches) && matches.size() > 1 && !_allowDuplicates) {
            List<TripMatchResult> dups = new ArrayList<>(matches);
            dups.sort(Collections.reverseOrder());
            TripMatchResult best = dups.get(0);
            for (int i = 1; i < dups.size(); i++) {
              TripMatchResult result = dups.get(i);
              _log.debug("dropping duplicate in static trip={}, RT trip={} ({}). Better trip is {} ({})",
                      best.getTripId(), result.getRtTripId(), result.getStatus(), best.getRtTripId(), best.getStatus());
              result.setStatus(Status.NO_MATCH);
              result.setResult(null);
            }
          }
        }

        Set<String> matchedTripIds = new HashSet<>();
        // Read out results of matching. If there is a match, rewrite TU's trip ID. Add TU to return list.
        for (TripMatchResult result : matchesByTrip.values()) {
          if (!result.getStatus().equals(Status.MERGED)) {
            GtfsRealtime.TripUpdate.Builder tub = result.getTripUpdateBuilder();
            GtfsRealtime.TripDescriptor.Builder tb = tub.getTripBuilder();
            GtfsRealtimeNYCT.NyctTripDescriptor extension = tb.getExtension(GtfsRealtimeNYCT.nyctTripDescriptor);
            if (extension != null && !extension.getIsAssigned() && dropUnassigned()) {
              _log.error("dropping unassigned trip={}", result.getTripUpdate().getTrip().getTripId());
              continue;
            }

            if (result.hasResult() && (result.getTripUpdate().getStopTimeUpdateCount() == 0 || !result.stopsMatchToEnd())) {
              _log.info("no stop match rt={} static={} {}",
                      result.getTripUpdate().getTrip().getTripId(), result.getResult().getTrip().getId().getId(),
                      (result.getResult().getStopTimes().get(0).getDepartureTime() / 60) * 100);
              result.setStatus(Status.NO_MATCH);
              result.setResult(null);
            }
            if (result.hasResult()) {
              ActivatedTrip at = result.getResult();
              String staticTripId = at.getTrip().getId().getId();
              _log.debug("matched {} -> {}", tb.getTripId(), staticTripId);
              tb.setTripId(staticTripId);
              removeTimepoints(at, tub);
              matchedTripIds.add(staticTripId);
            } else {
              _log.debug("unmatched: {} due to {}", tub.getTrip().getTripId(), result.getStatus());
              tb.setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.ADDED);
              // ignore ADDED trips without stops
              if (tub.getStopTimeUpdateCount() == 0)
                continue;
              // Trip Headsign and direction
              String stopId = result.getRtLastStop();
              String tripHeadsign = _tripActivator.getStopNameForId(stopId);
              NyctTripId nyctTripId = NyctTripId.buildFromTripDescriptor(tub.getTrip(), _routesWithReverseRTDirections);

              if (tub == null || tub.getTrip() == null || nyctTripId == null) {
                _log.error("missing trip for headsign " + tripHeadsign);
                continue;
              }
              String nsDirection = NyctTripId.buildFromTripDescriptor(tub.getTrip(), _routesWithReverseRTDirections).getDirection();
              String tripDirection = "S".equals(nsDirection) ? "1" : "0";
              GtfsRealtimeOneBusAway.OneBusAwayTripUpdate.Builder obaTripUpdate =
                      GtfsRealtimeOneBusAway.OneBusAwayTripUpdate.newBuilder();
              if(StringUtils.isNotBlank(tripHeadsign)) {
                obaTripUpdate.setTripHeadsign(tripHeadsign);
                //Stop Headsign
                if(_directionsService !=null)
                  _directionsService.fillStopHeadSigns(tub.getStopTimeUpdateBuilderList());
              }
              obaTripUpdate.setTripDirection(tripDirection);
              tub.setExtension(GtfsRealtimeOneBusAway.obaTripUpdate, obaTripUpdate.build());
            }
            tub.setTimestamp(timestamp);
            TripUpdate tripUpdate = tub.build();
            ret.add(tripUpdate);
          }

          routeMetrics.add(result);
          feedMetrics.add(result);
          totalMetrics.add(result);
        }

        if (_cancelUnmatchedTrips) {
          Iterator<ActivatedTrip> staticTrips = _tripActivator.getTripsForRangeAndRoute(start, end, routeId).iterator();
          while (staticTrips.hasNext()) {
            ActivatedTrip at = staticTrips.next();
            if (!matchedTripIds.contains(at.getTrip().getId().getId())) {
              long time = fm.getHeader().getTimestamp();
              if (at.activeFor(trp, time)) {
                TripUpdate.Builder tub = TripUpdate.newBuilder();
                TripDescriptor.Builder tdb = tub.getTripBuilder();
                tdb.setTripId(at.getTrip().getId().getId());
                tdb.setRouteId(at.getTrip().getRoute().getId().getId());
                tdb.setStartDate(at.getServiceDate().getAsString());
                tdb.setScheduleRelationship(ScheduleRelationship.CANCELED);
                ret.add(tub.build());

                routeMetrics.addCancelled();
                feedMetrics.addCancelled();
                totalMetrics.addCancelled();
              }
            }
          }
        }

        if (_listener != null)
          _listener.reportMatchesForRoute(routeId, routeMetrics, _cloudwatchNamespace);
        }
      }

    if (_listener != null)
      _listener.reportMatchesForSubwayFeed(feedId.toString(), feedMetrics, _cloudwatchNamespace);

    _log.info("feed={}, expired TUs={}", feedId, nExpiredTus);
    return ret;
  }

  private boolean dropUnassigned() {
    return "true".equals(System.getProperty("tripUpdateProcess.dropUnassigned"));
  }

  // TU is *expired* if the latest arrival or departure is 5 minutes before feed's timestamp
  private static boolean expiredTripUpdate(GtfsRealtime.TripUpdate tu, long timestamp) {
    OptionalLong latestTime = tu.getStopTimeUpdateList()
            .stream()
            .map(stu -> stu.hasDeparture() ? stu.getDeparture() : stu.getArrival())
            .filter(GtfsRealtime.TripUpdate.StopTimeEvent::hasTime)
            .mapToLong(GtfsRealtime.TripUpdate.StopTimeEvent::getTime).max();
    return latestTime.isPresent() && latestTime.getAsLong() < timestamp - 300;
  }

  // Remove StopTimeUpdate from TU if the stop is not in trip's list of stops.
  // NOTE this will remove timepoints, but remove additional stops for express trips that are running local.
  private void removeTimepoints(ActivatedTrip trip, GtfsRealtime.TripUpdate.Builder tripUpdate) {
    Set<String> stopIds = trip.getStopTimes().stream()
            .map(s -> s.getStop().getId().getId()).collect(Collectors.toSet());
    for(int i = 0; i < tripUpdate.getStopTimeUpdateCount(); i++) {
      String id = tripUpdate.getStopTimeUpdate(i).getStopId();
      if (!stopIds.contains(id)) {
        tripUpdate.removeStopTimeUpdate(i);
        i--;
      }
    }
  }

  // remove all stops NOT in static data
  private void removeTimepoints(TripUpdate.Builder tripUpdate) {
    for(int i = 0; i < tripUpdate.getStopTimeUpdateCount(); i++) {
      String id = tripUpdate.getStopTimeUpdate(i).getStopId();
      String routeId = tripUpdate.getTrip().getRouteId();
      if (!_tripActivator.isStopInStaticData(id)
              || (_stopFilterStrategy != null && !_stopFilterStrategy.shouldInclude(routeId, id))) {
        tripUpdate.removeStopTimeUpdate(i);
        i--;
      }
    }
  }

  // Due to a bug in I-TRAC's GTFS-RT output, there are distinct trip updates
  // for trips which have mid-line crew relief (route D).
  // The mid-line relief points are in the train ID so we can reconstruct
  // the whole trip if those points match.
  /** return true if merged, false otherwise */
  private boolean tryMergeResult(Collection<TripMatchResult> col) {
    if (col.size() != 2)
      return false;
    Iterator<TripMatchResult> iter = col.iterator();
    return mergedResult(iter.next(), iter.next()) != null;
  }

  public TripMatchResult mergedResult(TripMatchResult first, TripMatchResult second) {
    NyctTripId firstId = NyctTripId.buildFromTripDescriptor(first.getTripUpdate().getTrip(), _routesWithReverseRTDirections);
    NyctTripId secondId = NyctTripId.buildFromTripDescriptor(second.getTripUpdate().getTrip(), _routesWithReverseRTDirections);
    if (firstId.getOriginDepartureTime() > secondId.getOriginDepartureTime())
      return mergedResult(second, first);

    // Relief Point checks to see if the destination of the first trip matches the origin of the second trip

    // Destination first trip
    String midpt0 = getReliefPoint(first.getTripUpdate(), 1);

    // Origin second trip
    String midpt1 = getReliefPoint(second.getTripUpdate(), 0);

    // Destination and Origin match so must be part of the same trip update
    if (midpt0 != null && midpt0.equals(midpt1)) {

      // First trip updates
      GtfsRealtime.TripUpdate.Builder firstTripUpdate = first.getTripUpdateBuilder();

      // Second trip updates
      GtfsRealtime.TripUpdate.Builder secondTripUpdate = second.getTripUpdateBuilder();

      int firstTripStopTimesCount = firstTripUpdate.getStopTimeUpdateCount();
      int secondTripStopTimesCount = secondTripUpdate.getStopTimeUpdateCount();

      // Check to make sure both trips have stop time updates
      if(firstTripStopTimesCount <= 0 || secondTripStopTimesCount <= 0){
        return null;
      }

      Iterator<StopTimeUpdate.Builder> secondTripStusToAdd = secondTripUpdate.getStopTimeUpdateBuilderList().iterator();

      // last stop time update for first trip
      StopTimeUpdate.Builder firstTripLastStopTimeUpdate =  firstTripUpdate.getStopTimeUpdateBuilder(firstTripStopTimesCount - 1);;

      // first stop time update for second trip
      StopTimeUpdate.Builder secondTripFirstStopTimeUpdate = secondTripStusToAdd.next();

      //If last stop of first trip and first stop of second trip are same then all second trip stop times are merged
      // into first trip
      if(secondTripFirstStopTimeUpdate.getStopId().equals(firstTripLastStopTimeUpdate.getStopId())){
        firstTripLastStopTimeUpdate.setDeparture(secondTripFirstStopTimeUpdate.getDeparture());
        while (secondTripStusToAdd.hasNext()) {
          firstTripUpdate.addStopTimeUpdate(secondTripStusToAdd.next());
        }
        second.setStatus(Status.MERGED);
        return first;
      }
    }
    return null;
  }

   /**
    * Checks if last stop of first trip and first stop of second trip are same if stops match then trips are merged
    */
  private static String getReliefPoint(GtfsRealtime.TripUpdateOrBuilder update, int pt) {
    String trainId = update.getTrip().getExtension(GtfsRealtimeNYCT.nyctTripDescriptor).getTrainId();
    NyctTrainId parsedTrainId = NyctTrainId.buildFromString(trainId);

    if (parsedTrainId == null)
      return null;

    String[] points = new String[]{parsedTrainId.getOrigin(), parsedTrainId.getDestination()};
    if (pt >= points.length)
      return null;
    return points[pt];
  }

  private void reportRecordsIn(int recordsIn, int expiredUpdates, MatchMetrics... metrics) {
    for (MatchMetrics m : metrics) {
      m.reportRecordsIn(recordsIn);
      m.reportExpiredUpdates(expiredUpdates);
    }
  }
}
