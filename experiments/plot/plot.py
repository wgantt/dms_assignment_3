import matplotlib.pyplot as plt
from math import log

fig = plt.figure()
ax = plt.axes()

bsort = [0.056, 0.059, 0.086, 0.186, 0.484, 1.389, 4.303]
msort_poor = [0.072, 0.244, 0.878, 3.382, 11.288, 40.537, 134.523]
msort_good = [0.014, 0.056, 0.183, 0.607, 1.950, 8.409, 25.199]
file_size = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0]

# plt.plot(bsort, file_size, color ='#1f77b4', label='bsort')
# plt.plot(msort_poor, file_size, color ='#d62728', label='msort poorly-chosen')
# plt.plot(msort_good, file_size, color ='#2ca02c', label='msort well-tuned')

plt.plot(file_size, [log(y,10) for y in bsort], color ='#1f77b4', label='bsort')
plt.plot(file_size, [log(y,10) for y in msort_poor], color ='#d62728', label='msort poorly-chosen')
plt.plot(file_size, [log(y,10) for y in msort_good], color ='#2ca02c', label='msort well-tuned')
ax.legend()

plt.title("Runtime by file size for Bsort vs. Msort")
plt.xlabel("File size (# records, power of 10)")
plt.ylabel("Runtime (log(s))")

plt.savefig('part5.png')
