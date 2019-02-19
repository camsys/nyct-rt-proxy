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

import com.google.inject.Provider;
import org.onebusaway.gtfs.impl.GtfsDataServiceImpl;
import org.onebusaway.gtfs.impl.GtfsRelationalDaoImpl;
import org.onebusaway.gtfs.serialization.GtfsReader;
import org.onebusaway.gtfs.services.GtfsDataService;
import org.onebusaway.gtfs.services.GtfsRelationalDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;

public class GtfsDataServiceProvider implements Provider<GtfsDataService> {

  private static final Logger _log = LoggerFactory.getLogger(GtfsDataServiceProvider.class);

  @Inject
  @Named("NYCT.gtfsPath")
  private File _gtfsPath;

  public void setGtfsPath(File gtfsPath) {
    _gtfsPath = gtfsPath;
  }

  @Override
  public GtfsDataService get() {
    _log.info("Loading GTFS from {}", _gtfsPath.toString());
    GtfsRelationalDaoImpl dao = new GtfsRelationalDaoImpl();
    GtfsReader reader = new GtfsReader();
    reader.setEntityStore(dao);
    try {
      reader.setInputLocation(_gtfsPath);
      reader.run();
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Failure while reading GTFS", e);
    }
    GtfsDataServiceImpl gtfsDataService = new GtfsDataServiceImpl();
    gtfsDataService.setGtfsDao(dao);
    return gtfsDataService;
  }
}
