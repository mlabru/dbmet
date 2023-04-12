import numpy as np

from matplotlib import pyplot
import matplotlib.pyplot as plt
from matplotlib.cm import get_cmap
from matplotlib.colors import from_levels_and_colors

from cartopy import crs
from cartopy.feature import NaturalEarthFeature, COLORS

from netCDF4 import Dataset

from wrf import (getvar, to_np, get_cartopy, latlon_coords, vertcross,
                 cartopy_xlim, cartopy_ylim, interpline, CoordPair)

wrf_file = Dataset("C:/Users/meteorologia.gmmc/Documents/WRF - Rodada Exemplo/wrfout_exemplo.nc")

# Define the cross section start and end points
cross_start = CoordPair(lat=-8.5, lon=-42)
cross_end = CoordPair(lat=-8.5, lon=-30)

# Get the WRF variables
ht = getvar(wrf_file, "z", timeidx=8)
ht = ht[0:20,:,:]
ter = getvar(wrf_file, "ter", timeidx=8)

w = getvar(wrf_file, "wa", timeidx=8)
w = w[0:20,:,:]

u = getvar(wrf_file, "ua", timeidx=8)
u = u[0:20,:,:]

max_dbz = getvar(wrf_file, "mdbz", timeidx=8)

W = 10**(w/10.) # Use linear Z for interpolation

w_cross = vertcross(W, ht, wrfin=wrf_file,
                    start_point=cross_start,
                    end_point=cross_end,
                    latlon=True, meta=True)

U = 10**(u/10.) # Use linear Z for interpolation

u_cross = vertcross(U, ht, wrfin=wrf_file,
                    start_point=cross_start,
                    end_point=cross_end,
                    latlon=True, meta=True)

# Convert back to dBz after interpolation
w_cross = 10.0 * np.log10(w_cross)
u_cross = 10.0 * np.log10(u_cross)

# Add back the attributes that xarray dropped from the operations above
w_cross.attrs.update(w_cross.attrs)
w_cross.attrs["description"] = "destaggered w-wind component"
w_cross.attrs["units"] = "m s-1"

# Add back the attributes that xarray dropped from the operations above
u_cross.attrs.update(u_cross.attrs)
u_cross.attrs["description"] = "destaggered u-wind component"
u_cross.attrs["units"] = "m s-1"

# To remove the slight gap between the dbz contours and terrain due to the
# contouring of gridded data, a new vertical grid spacing, and model grid
# staggering, fill in the lower grid cells with the first non-missing value
# for each column.

# Make a copy of the z cross data. Let's use regular numpy arrays for this.
w_cross_filled = np.ma.copy(to_np(w_cross))
u_cross_filled = np.ma.copy(to_np(u_cross))

# For each cross section column, find the first index with non-missing
# values and copy these to the missing elements below.
for i in range(w_cross_filled.shape[-1]):
    column_vals = w_cross_filled[:,i]
    # Let's find the lowest index that isn't filled. The nonzero function
    # finds all unmasked values greater than 0. Since 0 is a valid value
    # for dBZ, let's change that threshold to be -200 dBZ instead.
    first_idx = int(np.transpose((column_vals > -200).nonzero())[0])
    w_cross_filled[0:first_idx, i] = w_cross_filled[first_idx, i]

# For each cross section column, find the first index with non-missing
# values and copy these to the missing elements below.
for i in range(u_cross_filled.shape[-1]):
    column_vals = u_cross_filled[:,i]
    # Let's find the lowest index that isn't filled. The nonzero function
    # finds all unmasked values greater than 0. Since 0 is a valid value
    # for dBZ, let's change that threshold to be -200 dBZ instead.
    first_idx = int(np.transpose((column_vals > -200).nonzero())[0])
    u_cross_filled[0:first_idx, i] = u_cross_filled[first_idx, i]

# Get the terrain heights along the cross section line
ter_line = interpline(ter, wrfin=wrf_file, start_point=cross_start,
                      end_point=cross_end)

# Get the lat/lon points
lats, lons = latlon_coords(w)

# Get the cartopy projection object
cart_proj = get_cartopy(w)

# Create the figure
fig = pyplot.figure(figsize=(8,6))
ax_cross = pyplot.axes()

# Make the cross section plot for dbz
w_levels = np.arange(-4E-1, +4E-1, 5E-2)
xs = np.arange(0, w_cross.shape[-1], 1)
ys = to_np(w_cross.coords["vertical"])
w_contours = ax_cross.contourf(xs,
                                 ys,
                                 to_np(w_cross_filled),
                                 levels=w_levels,
                                 cmap='seismic',

                                 extend="both")
# Add the color bar
cbar = fig.colorbar(w_contours, ax=ax_cross)
cbar.ax.tick_params(labelsize=12)
cbar.set_label('Vertical Velocity (m.s)', rotation=-270, fontsize=12)
# Fill in the mountain area
ht_fill = ax_cross.fill_between(xs, 0, to_np(ter_line),
                                facecolor="black")

# Tentativa do quiver

ax_cross.quiver(xs[::5], ys[::5],
          to_np(u_cross_filled[::5, ::5]), to_np(w_cross_filled[::5, ::5]*100))

# Set the x-ticks to use latitude and longitude labels
coord_pairs = to_np(u_cross.coords["xy_loc"])
x_ticks = np.arange(coord_pairs.shape[0])
x_labels = [pair.latlon_str() for pair in to_np(coord_pairs)]

# Set the desired number of x ticks below
num_ticks = 5
thin = int((len(x_ticks) / num_ticks) + .5)
ax_cross.set_xticks(x_ticks[::thin])
ax_cross.set_xticklabels(x_labels[::thin], rotation=90, fontsize=8)

# Set the x-axis and  y-axis labels
ax_cross.set_xlabel("Latitude, Longitude", fontsize=12)
ax_cross.set_ylabel("Altura (m)", fontsize=12)

# Add a title
ax_cross.set_title("Ilha com vetores zonais", {"fontsize" : 14})

pyplot.show()

fig.savefig('WV.png', dpi=None, facecolor='w', edgecolor='w',
        orientation='portrait', papertype=None, format=None,
        transparent=False, bbox_inches=None, pad_inches=0.1,
        frameon=None, metadata=None)
        