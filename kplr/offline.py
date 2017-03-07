import pandas as pd
import os, re
import glob

from .config import KPLR_ROOT
from .api import API, KOI, Star, LightCurve

class OfflineTable(object):
    def __init__(self, name):
        self.name = name
        self._df = None
        
    @property
    def filepath(self):
        return os.path.join(KPLR_ROOT,'data','tables',
                            '{}.csv'.format(self.name))
        
    @property
    def df(self):
        if self._df is None:
            self._df = pd.read_csv(self.filepath, comment='#')
        return self._df
       
class OfflineAPI(API):
    
    def _offline_table(self, name):
        if not hasattr(self, '_tables'):
            self._tables = {}
        if name not in self._tables:
            self._tables[name] = OfflineTable(name)
        return self._tables[name]
    
    def table_query(self, name, query):
        return self._offline_table(name).df.query(query)
    
    def koi(self, koi_number, **params):
        df = self.table_query('cumulative', 
                                "kepoi_name=='K{:08.2f}'".format(koi_number))
        if not len(df):
            raise ValueError("No KOI found with the number: '{0}'"
                             .format(koi_number))

        return KOI(self, {c:df[c].values[0] for c in df.columns})
    
    def star(self, kepid):
        df = self.table_query('q1_q17_dr25_stellar',
                             "kepid=={:.0f}".format(kepid))
        if not len(df):
            raise ValueError("No KIC target found with id: '{0}'"
                             .format(kepid))
        params = {c:df[c].values[0] for c in df.columns}
        params['kic_kepler_id'] = kepid
        return Star(self, params)
    
    def light_curves(self, kepler_id=None, short_cadence=True, fetch=False,
                     clobber=False, async=False, **params):
        """Returns light curves that are already available offline for kepler_id
        
        Current hack will only return long cadence lightcurve data 
        """
        datadir = os.path.join(KPLR_ROOT,'data','lightcurves',
                               '{:09d}'.format(kepler_id))
        lcs = []
        for f in glob.glob(os.path.join(datadir, '*_llc.fits')):
            m = re.search('(kplr\d+.*)_llc\.fits', f)
            if m:
                name = m.group(1)
                
                # Hack in the minimally necessary parameters
                params = {'sci_data_set_name':name,
                         'ktc_target_type':'LC',
                         'ktc_kepler_id':kepler_id}
                lcs.append(LightCurve(self, params))

        return lcs
                     