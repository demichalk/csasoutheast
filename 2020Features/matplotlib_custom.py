# Databricks notebook source
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats
mu = 0
std = 1
x = np.linspace(start=-4, stop=4, num=100)
y = stats.norm.pdf(x, mu, std) 
plt.plot(x, y)
plt.show()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
fig = plt.figure()
ax = plt.axes()
x = np.linspace(0, 5, 100)
plt.plot(x, np.sin(x), color='Indigo', linestyle='--', linewidth=3)
plt.grid(b=True, color='aqua', alpha=0.3, linestyle='-.', linewidth=2)
plt.show()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
x = np.linspace(0, 7, 100)
line1, = plt.plot(x, np.sin(x), label='sin')
line2, = plt.plot(x, np.cos(x), label='cos')
plt.legend(handles=[line1, line2], loc='lower right')
#major grid lines
plt.grid(b=True, which='major', color='gray', alpha=0.6, linestyle='dashdot', lw=1.5)
#minor grid lines
plt.minorticks_on()
plt.grid(b=True, which='minor', color='beige', alpha=0.8, ls='-', lw=1)
plt.show()

# COMMAND ----------

import pandas as pd
iris = pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")
ax = iris.plot()
print("here is a plot")
display(ax)
print("here is a table")
display(iris)