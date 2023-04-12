obs_times = pd.to_datetime(debertWeatherStation['Date/Time (LST)'])
wind_speed = debertWeatherStation['Wind Spd (km/h)']
wind_direction = debertWeatherStation['Wind Dir (10s deg)']*10.0

plt.figure(figsize=(14,8))
ax1 = plt.subplot(2, 1, 1)
plt.plot(obs_times,wind_speed,'-b',alpha=0.6)
plt.setp(ax1.get_xticklabels(), visible=False)
plt.grid()
plt.title("Wind Speed")
ax2 = plt.subplot(2, 1, 2,sharex=ax1)
arrow_scaler = 1
colors = plt.cm.jet(np.linspace(wind_speed.min(),wind_speed.max(), len(wind_speed)))
for i in range(0,len(obs_times),1):
    u = arrow_scaler*-1*np.sin((np.pi/180)*(wind_direction[i]))
    v = arrow_scaler*-1*np.cos((np.pi/180)*(wind_direction[i]))
    ax2.arrow(obs_times[i], 0, u, v, fc=colors[int(wind_speed[i])], ec='k', 
              head_width=0.2, head_length=0.5, width=0.2, length_includes_head=True, 
              alpha=0.6)
plt.ylim(-1.5,1.5)
plt.grid()
plt.title("Wind Direction")
