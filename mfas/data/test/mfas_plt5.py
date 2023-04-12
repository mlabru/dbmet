from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

z = np.linspace(0, 10000, 100)
t = [datetime.utcnow() + timedelta(minutes=15) * i for i in range(40)]
data = z[:, None] * np.arange(40)
plt.contour(t, z, data)
plt.xticks(rotation=45)
plt.show()
