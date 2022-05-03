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

import com.kurtraschke.nyctrtproxy.model.MatchMetrics;
import org.onebusaway.cloud.api.ExternalServices;
import org.onebusaway.cloud.api.ExternalServicesBridgeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Obtain metrics per-route and per-feed, and report to Cloudwatch.
 *
 * If cloudwatch credentials are not included in the configuration, this class will simply log.
 *
 * @author Simon Jacobs
 */
public class CloudwatchProxyDataListener implements ProxyDataListener {
  private static final Logger _log = LoggerFactory.getLogger(CloudwatchProxyDataListener.class);

  private boolean _disabled = false;

  private boolean _verbose = false;

  private ExternalServices _externalServices = new ExternalServicesBridgeFactory().getExternalServices();

  @Override
  public void reportMatchesForRoute(String routeId, MatchMetrics metrics, String namespace) {
    if (!reportMatches("route", routeId, metrics, namespace))
      _log.info("Cloudwatch: no data reported for route={}", routeId);
    _log.info("route={}, nMatchedTrips={}, nAddedTrips={}, nCancelledTrips={}, nDuplicates={}, nMergedTrips={}",routeId, metrics.getMatchedTrips(), metrics.getAddedTrips(), metrics.getCancelledTrips(), metrics.getDuplicates(), metrics.getMergedTrips());
  }

  @Override
  public void reportMatchesForSubwayFeed(String feedId, MatchMetrics metrics, String namespace) {
    if (!reportMatches("feed", feedId, metrics, namespace))
      _log.info("Cloudwatch: no data reported for feed={}", feedId);
    _log.info("feed={}, nMatchedTrips={}, nAddedTrips={}, nCancelledTrips={}, nDuplicates={}, nMergedTrips={}", feedId, metrics.getMatchedTrips(), metrics.getAddedTrips(), metrics.getCancelledTrips(), metrics.getDuplicates(), metrics.getMergedTrips());
  }

  @Override
  public void reportMatchesTotal(MatchMetrics metrics, String namespace) {
    if (!reportMatches(null, null, metrics, namespace))
      _log.info("Cloudwatch: no data reported for total metrics.");
    _log.info("total: nMatchedTrips={}, nAddedTrips={}, nCancelledTrips={}, nDuplicates={}, nMergedTrips={}", metrics.getMatchedTrips(), metrics.getAddedTrips(), metrics.getCancelledTrips(), metrics.getDuplicates(), metrics.getMergedTrips());
  }


  private boolean reportMatches(String dimensionName, String dimensionValue, MatchMetrics metrics, String namespace) {
    if (isDisabled())
      return false;

    MetricValues metricValues = new MetricValues();

    // report latency metric
    if (metrics.getLatency() >= 0) {
      metricValues.addValues("Latency", dimensionName, dimensionValue, metrics.getLatency());
    }

    if (metrics.getMatchedTrips() + metrics.getAddedTrips() > 0) {

      // non verbose
      metricValues.addValues( "RecordsIn", dimensionName, dimensionValue, metrics.getRecordsIn());
      metricValues.addValues( "ExpiredUpdates", dimensionName, dimensionValue, metrics.getExpiredUpdates());
      metricValues.addValues( "MatchedTrips", dimensionName, dimensionValue, metrics.getMatchedTrips());
      metricValues.addValues( "AddedTrips", dimensionName, dimensionValue, metrics.getAddedTrips());
      metricValues.addValues( "CancelledTrips", dimensionName, dimensionValue, metrics.getCancelledTrips());
      metricValues.addValues( "MergedTrips", dimensionName, dimensionValue, metrics.getMergedTrips());
      metricValues.addValues( "RecordsOut", dimensionName, dimensionValue, metrics.getRecordsOut());

      if (_verbose) {
        double nRt = metrics.getMatchedTrips() + metrics.getAddedTrips();
        double nMatchedRtPct = ((double) metrics.getMatchedTrips()) / nRt;

        double nUnmatchedWithoutStartDatePct = ((double) metrics.getUnmatchedNoStartDate()) / nRt;
        double nUnmatchedNoStopMatchPct = ((double) metrics.getUnmatchedNoStopMatch()) / nRt;
        double nStrictMatchPct = ((double) metrics.getStrictMatch()) / nRt;
        double nLooseMatchSameDayPct = ((double) metrics.getLooseMatchSameDay()) / nRt;
        double nLooseMatchOtherDayPct = ((double) metrics.getLooseMatchOtherDay()) / nRt;
        double nLooseMatchCoercionPct = ((double) metrics.getLooseMatchCoercion()) / nRt;
        double nMergedPct = ((double) metrics.getMergedTrips()) / nRt;

        metricValues.addValues( "DuplicateTripMatches", dimensionName, dimensionValue, metrics.getDuplicates());
        metricValues.addValues( "UnmatchedBadId", dimensionName, dimensionValue, metrics.getBadId());
        metricValues.addValues( "MatchedRtTripsPct", dimensionName, dimensionValue, nMatchedRtPct);
        metricValues.addValues( "UnmatchedWithoutStartDatePct", dimensionName, dimensionValue, nUnmatchedWithoutStartDatePct);
        metricValues.addValues( "UnmatchedNoStopMatchPct", dimensionName, dimensionValue, nUnmatchedNoStopMatchPct);
        metricValues.addValues( "StrictMatchPct", dimensionName, dimensionValue, nStrictMatchPct);
        metricValues.addValues( "LooseMatchSameDayPct", dimensionName, dimensionValue, nLooseMatchSameDayPct);
        metricValues.addValues( "LooseMatchOtherDayPct", dimensionName, dimensionValue, nLooseMatchOtherDayPct);
        metricValues.addValues( "LooseMatchCoercionPct", dimensionName, dimensionValue,  nLooseMatchCoercionPct);
        metricValues.addValues( "MergedTripsPct", dimensionName, dimensionValue, nMergedPct);
      }
    }

    if (_externalServices.isInstancePrimary()) {

      _externalServices.publishMetrics(namespace, metricValues.getMetricNames(), metricValues.getDimensionNames(), metricValues.getDimensionValues(), metricValues.getValues());
    }

    return true;
  }

  private class MetricValues{
    private ArrayList<String> metricNames = new ArrayList<>();
    private ArrayList<String> dimensionNames = new ArrayList<>();
    private ArrayList<String> dimensionValues = new ArrayList<>();
    private ArrayList<Double> values = new ArrayList<>();

    public void addValues(String metricName, String dimensionName, String dimensionValue, double value){
      metricNames.add(metricName);
      dimensionNames.add(dimensionName);
      dimensionValues.add(dimensionValue);
      values.add(value);
    }

    public ArrayList<Double> getValues() {
      return values;
    }
    public ArrayList<String> getDimensionNames() {
      return dimensionNames;
    }
    public ArrayList<String> getDimensionValues() {
      return dimensionValues;
    }
    public ArrayList<String> getMetricNames() {
      return metricNames;
    }
  }

  public void setVerbose(boolean verbose) {
    _verbose = verbose;
  }

  public void setDisabled(boolean disabled) {
    _disabled = disabled;
  }

  public boolean isDisabled() {
    return _disabled;
  }
}
