/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kurtraschke.nyctrtproxy.services;

import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.calendar.CalendarServiceData;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.onebusaway.gtfs.services.GtfsRelationalDao;

import com.google.common.collect.ImmutableSet;
import com.kurtraschke.nyctrtproxy.model.ActivatedTrip;
import com.vividsolutions.jts.index.strtree.SIRtree;

import java.util.Date;
import java.util.IntSummaryStatistics;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

/**
 *
 * @author kurt
 */
public class TripActivator {

  private CalendarServiceData _csd;

  private GtfsRelationalDao _dao;

  private SIRtree tripTimesTree;

  private int maxLookback;

  @Inject
  public void setCalendarServiceData(CalendarServiceData csd) {
    _csd = csd;
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

      tripTimesTree.insert(iss.getMin(), iss.getMax(), new TripWithTime(trip, iss.getMin(), iss.getMax()));
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

              int sdOrigin = (int) (sd.getAsCalendar(_csd.getTimeZoneForAgencyId("MTA NYCT")).getTimeInMillis() / 1000);

              int startTime = (int) ((start.getTime() / 1000) - sdOrigin);
              int endTime = (int) ((end.getTime() / 1000) - sdOrigin);

              Stream<TripWithTime> tripsStream = tripTimesTree.query(startTime, endTime).stream()
                      .map(TripWithTime.class::cast);

              return tripsStream
                      .filter(t -> routeIds.contains(t.trip.getRoute().getId().getId()))
                      .filter(t -> serviceIdsForDate.contains(t.trip.getServiceId()))
                      .map(t -> new ActivatedTrip(sd, t.trip, t.start, t.end));
            });

  }

  public Stream<ActivatedTrip> getTripsForRangeAndRoute(Date start, Date end, String routeId) {
    return getTripsForRangeAndRoutes(start, end, ImmutableSet.of(routeId));
  }

}

class TripWithTime {
  Trip trip;
  long start;
  long end;
  TripWithTime(Trip trip, long start, long end) {
    this.trip = trip;
    this.start = start;
    this.end = end;
  }
  @Override
  public int hashCode() {
    return trip.hashCode();
  }
  @Override
  public boolean equals(Object o) {
    return (o instanceof TripWithTime) && trip.equals(((TripWithTime) o).trip);
  }
}