# this code was copied from a Jupyter notebook from Rich Signell's youtube
# video: https://www.youtube.com/watch?v=2TkZa0s58qs
import xarray as xr
import s3fs
from geoviews import opts
import hvplot.pandas
import hvplot.xarray
import geoviews as gv

fs = s3fs.S3FileSystem(anon=True)
s3map = s3fs.S3Map('esip-pangeo-uswest2/pangeo/NWM/2010', s3=fx)
ds = xr.open_zarr(s3map)

fs = s3fs.S3FileSystem(anon=True, default_fill_cache=False)
fileObj = fs.open('esip-pangeo-uswest2/pangeo/NWM/nwm-v1.2-channel_spatial_index.nc')

ds_lonlat=xr.open_dataset(fileObj)

ds.coords['latitude'] = ds_lonlat['latitude']
ds.coords['longitude'] = ds_lonlat['longitude']

ds.time[-1]

ds.streamflow

ds.streamflow[:, 0].hvplot()

import warnings
warnings.filterwarnings("ignore")

from dask.distributed import Client, progress, LocalCluster
from dask_kubernetes import KubeCluster

cluster = KubeCluster()
cluster.scale(25)
cluster
client = Client(cluster)

var = 'streamflow'

ds[var].nbytes/1e9

var_mean = ds[var].mean(dim='time').persist()
progress(var_mean)

df = var_mean.to_pandas().to_frame()

df = df.assign(latitude=df['latitude'].values)
df = df.assign(longitude=df['longitude'].values)
df.rename(columns={0:"transport"}, inplace=True)

from holoviews.operation.datashader import datashade, shade, dynspread, rasterize, spread

import cartopy.crs as ccrs

p = df.hvplot.points('longitude', 'latitude', crs=ccrs.PlateCarree(),
                     c='transport', colorbar=True, size=14)
g = rasterize(p, aggregator='mean', x_sampling=0.02, y_sampling=0.02,
              width=500).opts(tools=['hover'])
