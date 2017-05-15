# pTimeStats

Shell utility for computing statistics on the variability of elapsed time when running an application.

## Help & documentation

For getting help:  
`$> python pTimeStats.py -h`

For getting the documentation  
`$> python pTimeStats.py --doc`

## Examples

Consider for instance a command  
`spark-submit availability_by_town.py --num-part 4`  
the command below runs 30 times the command above and computes statistics:  
`$> python pTimeStats.py command 'spark-submit availability_by_town.py --num-part 4' 30`

For creating a log file `pTimeStats.log` with logs of the runs, use the `--log` option:  
`$> python pTimeStats.py --log command 'spark-submit availability_by_town.py --num-part 4' 30`

For computing stats from a log file created with a previous execution of the `python pTimeStats.py command` use the command `python pTimeStats.py process-log`:  
`$> python pTimeStats.py process-log pTimeStats.log`
