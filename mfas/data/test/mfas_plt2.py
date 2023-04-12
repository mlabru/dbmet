import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

u = np.array([1, 2, 3, 5, 7, 7, 7, 7])
v = np.array([-1, -1, -1, 1, 3, 3, 3, 3])
z = np.array([2, 10, 50, 100, 200, 300, 400, 500])

fig,ax = plt.subplots(figsize=[15, 8], ncols=2, sharey=False)
ax[0].plot(u, z, label='U-component')
ax[0].plot(v, z, label='V-component')
ax[0].axvline(0, color='k')
ax[0].legend(loc=4)

# Xq, Yq = np.meshgrid(1, np.arange(0, u.shape[0]))
Xq, Yq = np.meshgrid(1, z)

ax[1].barbs(Xq, Yq, u, v)

plt.show()
