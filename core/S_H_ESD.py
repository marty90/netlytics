from seasonal import fit_seasons, adjust_seasons
import numpy as np
import math
from scipy.stats import t
import numpy.ma as ma
import six.moves as smo

def S_H_ESD (s, period=None, alpha=0.025, hybrid=True ):

    seasons, trend = fit_seasons(s, period = period)
    adjusted = adjust_seasons(s, seasons=seasons)
    residual = adjusted - trend

    max_out = int(len(residual)/2-1)

    outliers = generalizedESD(residual, maxOLs=max_out, alpha=alpha,hybrid=hybrid)[1]

    return (seasons, trend, residual, outliers)


# Thank to https://github.com/sczesla/PyAstronomy
def generalizedESD(x, maxOLs, alpha=0.05, fullOutput=False, hybrid=True):
 
  xm = ma.array(x)
  n = len(xm)
  # Compute R-values
  R = []
  L = []
  minds = []
  for i in smo.range(maxOLs + 1):
    # Compute mean and std of x
    if hybrid:
        xmean = np.median(xm) #xm.mean()
        xstd = np.median ( np.abs(xm - np.median(xm)) )
    else:
        xmean = xm.mean()
        xstd = xm.std()
    # Find maximum deviation
    rr = np.abs((xm - xmean)/xstd)
    minds.append(np.argmax(rr))
    R.append(rr[minds[-1]])
    if i >= 1:
      p = 1.0 - alpha/(2.0*(n - i + 1))
      perPoint = t.ppf(p, n-i-1)
      L.append((n-i)*perPoint / np.sqrt((n-i-1+perPoint**2) * (n-i+1)))
    # Mask that value and proceed
    xm[minds[-1]] = ma.masked
  # Remove the first entry from R, which is of
  # no meaning for the test
  R.pop(-1)
  # Find the number of outliers
  ofound = False
  for i in smo.range(maxOLs-1, -1, -1):
    if R[i] > L[i]:
      ofound = True
      break
  # Prepare return value
  if ofound:
    if not fullOutput:
      # There are outliers
      return i+1, minds[0:i+1]
    else:
      return i+1, minds[0:i+1], R, L, minds
  else:
    # No outliers could be detected
    if not fullOutput:
      # There are outliers
      return 0, []
    else:
      return 0, [], R, L, minds
