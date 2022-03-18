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
package com.kurtraschke.nyctrtproxy.model;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtimeNYCT;
import org.apache.commons.lang3.StringUtils;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GTFS or realtime trip identifier broken into constituent parts; most importantly, route, direction, and origin-departure time.
 *
 * Origin-departure time is encoded as hundredths of a minute after midnight.
 *
 * @author kurt
 */
public class NyctTripId {

  private static final Pattern _rtTripPattern = Pattern.compile(
          "([A-Z0-9]+_)?(?<originDepartureTime>[0-9-]{6})_?(?<route>[A-Z0-9]+)\\.+(?<direction>[NS]?)(?<network>[A-Z0-9 -]*)$");

  private static final Pattern _staticTripPattern = Pattern.compile(
          "(?<route>[A-Z0-9]+)\\.+(?<direction>[NS])(?<network>[A-Z0-9]*)$"
  );

  private static final Logger _log = LoggerFactory.getLogger(NyctTripId.class);

  private int originDepartureTime;
  private String pathId;
  private String directionId;
  private String routeId;
  private String networkId;

  public int getOriginDepartureTime() {
    return originDepartureTime;
  }

  public String getPathId() {
    return pathId;
  }

  public String getDirection() {
    return directionId;
  }

  public String getRouteId() {
    return routeId;
  }

  public String getNetworkId() {
    return networkId;
  }


  /**
   * Parse a trip ID (from static GTFS or realtime feed) into NyctTripId
   *
   * @param tripId the trip ID
   * @return parsed trip ID
   */
  public static NyctTripId buildFromString(String tripId) {
    int originDepartureTime;
    String pathId, routeId, directionId, networkId;

    Matcher matcher = _rtTripPattern.matcher(tripId);

    if (matcher.find()) {
      originDepartureTime = Integer.parseInt(matcher.group("originDepartureTime"), 10);
      pathId = StringUtils.rightPad(matcher.group("route"), 3, '.') + matcher.group("direction");
      routeId = matcher.group("route");
      directionId = matcher.group("direction");
      if (directionId.length() == 0)
        directionId = null;
      networkId = matcher.group("network");
      if (networkId.length() == 0)
        networkId = null;
      return new NyctTripId(originDepartureTime, pathId, routeId, directionId, networkId);

    } else {
      return null;
    }

  }

  /**
   * Build a NyctTripId from a static GTFS Trip.
   *
   * This is necessary because route W static trip IDs have "N" in the typical 'route' position.
   *
   * @param trip GTFS static trip
   * @return parsed trip ID
   */
  public static NyctTripId buildFromTrip(Trip trip) {
    NyctTripId id = buildFromString(trip.getId().getId());
    if (id != null)
      id.routeId = trip.getRoute().getId().getId();
    return id;
  }


   /**
    * Build a NyctTripId from a trip and stop times - we cannot count on tripIds in ATIS GTFS
    */
   public static NyctTripId buildFromGtfs(Trip trip, List<StopTime> stopTimes) {
     int originDepartureTime = (stopTimes.get(0).getDepartureTime() * 100) / 60;
     String pathId = trip.getMtaTripId();
     String routeId = trip.getRoute().getId().getId();
     String directionId = trip.getDirectionId().equals("0") ? "N" : "S";
     String networkId = null;
     if (pathId != null) {
       Matcher matcher = _staticTripPattern.matcher(pathId);
       if (matcher.find()) {
         networkId = matcher.group("network");
       } else throw new IllegalArgumentException("bad path ID");
     } else {
       // for tests- may as well check the trip ID as per non-ATIS GTFS
       NyctTripId other = buildFromString(trip.getId().getId());
       if (other != null) {
         pathId = other.pathId;
         networkId = other.networkId;
       }
     }
     return new NyctTripId(originDepartureTime, pathId, routeId, directionId, networkId);
   }

  /**
   * Build a NyctTripId from a TripDescriptor
   *
   * This is necessary because route 6X realtime trip IDs have "6" in the typical 'route' position.
   *
   * @param td GTFS-RT TripDescriptor
   * @return parsed trip ID
   */
  public static NyctTripId buildFromTripDescriptor(GtfsRealtime.TripDescriptorOrBuilder td) {
    return buildFromTripDescriptor(td, Collections.emptySet());
  }

  public static NyctTripId buildFromTripDescriptor(GtfsRealtime.TripDescriptorOrBuilder td, Set<String> reverseDirectionsRoutes) {
    NyctTripId id = buildFromString(td.getTripId());
    if (id != null) {
      if (td.hasRouteId()) {
        id.routeId = td.getRouteId();
      }
      if (id.getDirection() == null && (id.getRouteId().equals("7") || id.getRouteId().equals("7X"))) {
        id.directionId = inferFlushingDirection(td.getExtension(GtfsRealtimeNYCT.nyctTripDescriptor).getTrainId());
      }
      if (reverseDirectionsRoutes.contains(id.routeId)) {
        id.directionId = id.directionId.equals("N") ? "S" : "N";
      }
    }
    return id;
  }

  private NyctTripId(int originDepartureTime, String pathId, String routeId, String directionId, String networkId) {
    this.originDepartureTime = originDepartureTime;
    this.pathId = pathId;
    this.routeId = routeId;
    this.directionId = directionId;
    this.networkId = networkId;
  }

  @Override
  public String toString() {
    return String.format("%06d_%s", originDepartureTime, pathId);
  }

  /**
   * Check route, direction, and network ID match. Note only Feed 1 has IDs which *may* have network ID.
   *
   * This method throws an exception if {@link #networkId} is null.
   *
   * @param other
   * @return true if match, false otherwise
   */
  public boolean strictMatch(NyctTripId other) {
    return  looseMatch(other)
            && networkId != null
            && getNetworkId().equals(other.getNetworkId());
  }

  /**
   * Check route, direction, and origin-departure time match.
   *
   * @param other
   * @return true if match, false otherwise
   */
  public boolean looseMatch(NyctTripId other) {
    return routeDirMatch(other)
            && getOriginDepartureTime() == other.getOriginDepartureTime();
  }

  /**
   * Check route and direction match.
   *
   * @param other
   * @return true if match, false otherwise
   */
  public boolean routeDirMatch(NyctTripId other) {
    return getRouteId().equals(other.getRouteId())
            && getDirection().equals(other.getDirection());
  }

  /**
   * Obtain NyctTripId that is relative to a 26-hour schedule on the previous day, otherwise the same as this one.
   *
   * @return new trip ID
   */
  public NyctTripId relativeToPreviousDay() {
    int time = originDepartureTime + (24 * 60 * 100);
    return new NyctTripId(time, pathId, routeId, directionId, networkId);
  }

  /**
   * The Flushing ATS generates path IDs from the RTIF short names of the origin and destination stops, truncated to
   * fit within the path ID field. This results in either eight or nine characters being available for the stop names,
   * depending on whether the route ID is "7" or "7X". In the worst case, an express trip departing from a stop whose
   * RTIF short name is exactly eight characters long, there may be no room left for the destination stop name at all.
   *
   * Consequently, we instead attempt to infer direction of travel from the train ID, which is consistent with the
   * standard format.
   *
   * @param trainId train ID in NYCT standard format
   * @return direction of travel, "N" or "S"
   */
  @VisibleForTesting
  static String inferFlushingDirection(String trainId) {
    //Flushing Line stop abbreviations, ordered from north to south
    List<String> flushingStopAbbreviations = ImmutableList.of("MST", "WPT", "111", "103", "JCT", "90S", "82S", "74S", "69S", "61S", "52S", "46B", "40S", "RAW", "QBP", "CHS", "HTR", "VER", "G-C", "5AV", "TSQ", "34H");

    NyctTrainId parsedTrainId = NyctTrainId.buildFromString(trainId);

    if (parsedTrainId == null) {
      return null;
    }

    int originIndex = flushingStopAbbreviations.indexOf(parsedTrainId.getOrigin());
    int destinationIndex = flushingStopAbbreviations.indexOf(parsedTrainId.getDestination());

    if (originIndex == -1 || destinationIndex == -1 || originIndex == destinationIndex) {
      return null;
    }

    if (originIndex > destinationIndex) {
      return "N";
    } else {
      return "S";
    }

  }

}
