import matplotlib.pylab as plt
import numpy as np

x = np.linspace(-5, 5, 5)
X, Y = np.meshgrid(x, x)
d = np.arctan(Y ** 2. - .25 * Y - X)
U, V = 5 * np.cos(d), np.sin(d)

# barbs plot
ax1 = plt.subplot(1, 2, 1)
ax1.barbs(X, Y, U, V)
'''
# quiver plot
ax2 = plt.subplot(1, 2, 2)
qui = ax2.quiver(X, Y, U, V)
plt.quiverkey(qui, 0.9, 1.05, 1, '1 m/s', labelpos='E', fontproperties={'weight': 'bold'})
'''
plt.show()
