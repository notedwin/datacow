import cProfile
import pstats
import pyprof2calltree
import websites_ip

# run the profiler on the module
cProfile.run('websites_ip.main()', 'my_module_stats')

# # create a pstats.Stats object from the profiling data
stats = pstats.Stats('my_module_stats')

stats.strip_dirs().sort_stats('cumulative').print_stats(10)

