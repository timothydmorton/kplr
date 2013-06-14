#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import (division, print_function, absolute_import,
                        unicode_literals)

__all__ = ["API"]

import os
import re
import json
import urllib
import urllib2
import logging

from .config import KPLR_ROOT
from . import mast


class API(object):
    """
    Interface with MAST and Exoplanet Archive APIs.

    """

    ea_url = ("http://exoplanetarchive.ipac.caltech.edu/cgi-bin/nstedAPI"
              "/nph-nstedAPI")
    mast_url = "http://archive.stsci.edu/kepler/{0}/search.php"

    def __init__(self, data_root=None):
        self.data_root = data_root
        if data_root is None:
            self.data_root = KPLR_ROOT

    def ea_request(self, table, **params):
        """
        Submit a request to the Exoplanet Archive API and return a dictionary.

        :param table:
            The table that you want to search.

        :param params:
            Any other search parameters.

        """
        params["table"] = table

        # Format the URL in the *horrifying* way that EA needs it to be.
        payload = ["{0}={1}".format(k, urllib.quote_plus(v, "\"'+"))
                   for k, v in params.items()]

        # Send the request.
        r = urllib2.Request(self.ea_url, data="&".join(payload))
        handler = urllib2.urlopen(r)
        code = handler.getcode()
        if int(code) != 200:
            raise RuntimeError("The Exoplanet Archive returned {0}"
                               .format(code))
        txt = handler.read()

        # Hack because Exoplanet Archive doesn't return HTTP errors.
        if "ERROR" in txt:
            raise RuntimeError("The Exoplanet Archive failed with message:\n"
                               + txt)

        # Parse the CSV output.
        csv = txt.splitlines()
        columns = csv[0].split(",")
        result = []
        for line in csv[1:]:
            result.append(dict(zip(columns, line.split(","))))

        return [self._munge_dict(row) for row in result]

    def mast_request(self, category, adapter=None, **params):
        """
        Submit a request to the MAST API and return a dictionary of parameters.

        :param category:
            The table that you want to search.

        :param params:
            Any other search parameters.

        """
        params["action"] = params.get("action", "Search")
        params["outputformat"] = "JSON"
        params["coordformat"] = "dec"
        params["verb"] = 3
        if "sort" in params:
            params["ordercolumn1"] = params.pop("sort")

        # Send the request.
        r = urllib2.Request(self.mast_url.format(category),
                            data=urllib.urlencode(params))
        handler = urllib2.urlopen(r)
        code = handler.getcode()
        txt = handler.read()
        if int(code) != 200:
            raise RuntimeError("The MAST API returned {0} with message:\n {1}"
                               .format(code, txt))

        # Parse the JSON.
        result = json.loads(txt)

        # Fake munge the types if no adapter was provided.
        if adapter is None:
            return [self._munge_dict(row) for row in result]

        return [adapter(row) for row in result]

    def _munge_dict(self, row):
        """
        Iterate through a dictionary and try to interpret the data types in a
        sane way.

        :param row:
            A dictionary of (probably) strings.

        :returns new_row:
            A dictionary with the same keys as ``row`` but reasonably typed
            values.

        """
        tmp = {}
        for k, v in row.items():
            # Good-god-what-type-is-this-parameter?!?
            try:
                tmp[k] = int(v)
            except ValueError:
                try:
                    tmp[k] = float(v)
                except ValueError:
                    tmp[k] = v

            # Empty entries are mapped to None.
            if v == "":
                tmp[k] = None
        return tmp

    def kois(self, **params):
        """
        Get a list of KOIs from the Exoplanet Archive.

        :param **params:
            The search parameters for the Exoplanet Archive API.

        """
        return [KOI(self, k) for k in self.ea_request("cumulative", **params)]

    def koi(self, koi_number):
        """
        Find a single KOI given a KOI number (e.g. 145.01).

        :param koi_number:
            The number identifying the KOI. This should be a float with the
            ``.0N`` for some value of ``N``.

        """
        kois = self.kois(where="kepoi_name+like+'K{0:08.2f}'"
                         .format(float(koi_number)))
        if not len(kois):
            raise ValueError("No KOI found with the number: '{0}'"
                             .format(koi_number))
        return kois[0]

    def planets(self, **params):
        """
        Get a list of confirmed (Kepler) planets from MAST.

        :param **params:
            The search parameters for the MAST API.

        """
        planets = self.mast_request("confirmed_planets",
                                    adapter=mast.planet_adapter, **params)
        return [Planet(self, p) for p in planets]

    def planet(self, name):
        """
        Get a planet by the Kepler name (e.g. "6b" or "Kepler-62b").

        :param name:
            The name of the planet.

        """
        # Parse the planet name.
        matches = re.findall("([0-9]+)[-\s]*([a-zA-Z])", name)
        if len(matches) != 1:
            raise ValueError("Invalid planet name '{0}'".format(name))
        kepler_name = "Kepler-{0} {1}".format(*(matches[0]))

        # Query the API.
        planets = self.planets(kepler_name=kepler_name, max_records=1)
        if not len(planets):
            raise ValueError("No planet found with the name: '{0}'"
                             .format(kepler_name))
        return planets[0]

    def stars(self, **params):
        """
        Get a list of KIC targets from MAST. Only return up to 100 results by
        default.

        :param **params:
            The query parameters for the MAST API.

        """
        params["max_records"] = params.pop("max_records", 100)
        stars = self.mast_request("kic10", adapter=mast.star_adapter,
                                  **params)
        return [Star(self, s) for s in stars]

    def star(self, kepid):
        """
        Get a KIC target by id from MAST.

        :param kepid:
            The integer ID of the star in the KIC.

        """
        stars = self.stars(kic_kepler_id=kepid, max_records=1)
        if not len(stars):
            raise ValueError("No KIC target found with id: '{0}'"
                             .format(kepid))
        return stars[0]

    def _data_search(self, kepler_id, short_cadence=True):
        params = {"ktc_kepler_id": kepler_id}
        if not short_cadence:
            params["ktc_target_type"] = "LC"

        data_list = self.mast_request("data_search",
                                      adapter=mast.dataset_adapter,
                                      **params)
        if not len(data_list):
            raise ValueError("No data files found for: '{0}'"
                             .format(kepler_id))
        return data_list

    def light_curves(self, kepler_id, short_cadence=True):
        """

        """
        return [LightCurve(self, d) for d in self._data_search(kepler_id,
                short_cadence=short_cadence)]

    def target_pixel_files(self, kepler_id, short_cadence=True):
        """

        """
        return [TargetPixelFile(self, d) for d in self._data_search(kepler_id,
                short_cadence=short_cadence)]


class Model(object):

    _id = "{_id}"

    def __init__(self, api, params):
        self.api = api
        for k, v in params.iteritems():
            setattr(self, k, v)
        self._name = self._id.format(**params)

    def __str__(self):
        return "<{0}({1})>".format(self.__class__.__name__, self._name)

    def __unicode__(self):
        return self.__str__()

    def __repr__(self):
        return self.__str__()

    def get_light_curves(self, short_cadence=True):
        return self.api.light_curves(self.kepid, short_cadence=short_cadence)

    def get_target_pixel_files(self, short_cadence=True):
        return self.api.target_pixel_files(self.kepid,
                                           short_cadence=short_cadence)


class KOI(Model):

    _id = "\"{kepoi_name}\""

    def __init__(self, *args, **params):
        super(KOI, self).__init__(*args, **params)
        self._star = None

    @property
    def star(self):
        if self._star is None:
            self._star = self.api.star(self.kepid)
        return self._star


class Planet(Model):

    _id = "\"{kepler_name}\""

    def __init__(self, *args, **params):
        super(Planet, self).__init__(*args, **params)
        self._koi = None
        self._star = None

    @property
    def koi(self):
        if self._koi is None:
            self._koi = self.api.koi(self.koi_number)
        return self._koi

    @property
    def star(self):
        if self._star is None:
            self._star = self.api.star(self.kepid)
        return self._star


class Star(Model):

    _id = "{kic_kepler_id}"

    def __init__(self, *args, **params):
        super(Star, self).__init__(*args, **params)
        self.kepid = self.kic_kepler_id
        self._kois = None

    @property
    def kois(self):
        if self._kois is None:
            self._kois = self.api.kois(where="kepid like '{0}'"
                                       .format(self.kepid))
        return self._kois


class _datafile(Model):

    _id = "\"{sci_data_set_name}_{ktc_target_type}\""
    base_url = "http://archive.stsci.edu/pub/kepler/{0}/{1}/{2}/{3}"
    product = None
    suffixes = None
    filetype = None

    def __init__(self, *args, **params):
        super(_datafile, self).__init__(*args, **params)
        self.kepid = "{0:09d}".format(int(self.ktc_kepler_id))
        self.base_dir = os.path.join(self.api.data_root, "data", self.product,
                                     self.kepid)

        suffix = self.suffixes[int(self.ktc_target_type != "LC")]
        self._filename = "{0}_{1}{2}".format(self.sci_data_set_name,
                                             suffix, self.filetype).lower()

    @property
    def filename(self):
        return os.path.join(self.base_dir, self._filename)

    @property
    def url(self):
        return self.base_url.format(self.product, self.kepid[:4],
                                    self.kepid, self._filename)

    def fetch(self, clobber=False):
        # Check if the file already exists.
        filename = self.filename
        if os.path.exists(filename) and not clobber:
            logging.info("Found local file: '{0}'".format(filename))
            return self

        # Fetch the remote file.
        url = self.url
        logging.info("Downloading file from: '{0}'".format(url))
        r = urllib2.Request(url)
        handler = urllib2.urlopen(r)
        code = handler.getcode()
        if int(code) != 200:
            raise RuntimeError("{0}".format(code))

        # Make sure that the root directory exists.
        try:
            os.makedirs(self.base_dir)
        except os.error:
            pass

        # Save the contents of the file.
        logging.info("Saving file to: '{0}'".format(filename))
        open(filename, "wb").write(handler.read())

        return self


class LightCurve(_datafile):

    product = "lightcurves"
    suffixes = ["llc", "slc"]
    filetype = ".fits"


class TargetPixelFile(_datafile):

    product = "target_pixel_files"
    suffixes = ["lpd-targ", "spd-targ"]
    filetype = ".fits.gz"
