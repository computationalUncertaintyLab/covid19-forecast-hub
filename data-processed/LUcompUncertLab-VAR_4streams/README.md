# Lehigh University Computational Uncertainty Lab: VAR 3 Streams Model

## Usage

To run on the cluster, clone this repo on your sol account, or if it already exists, pull for the latest changes, and run
 ```
 git checkout ludev
 ```

 Then, navigate to this directory, and run
 ```
 sh distributor.sh
 ```

 To check on progress, the distributor will create files labeled ```myjob###```. Run 
 ```
 cat myjob###
 ``` 
 to get an instantaneous look on how any specific job is doing. Each job corresponds to one specific location out of 3200. Not all jobs will run simultaneously, but in my experience, up to 100 locations are run at a time.

 Once all locations run, the directory ```location_specific_forecasts/``` will contain the final forecast file and all intermediate steps. 
 
 The distributor will give one last job, which will execute ```lastSteps.sh```. It compiles all the FINAL files from ```location_specific_forecasts/``` into one main csv file, ready to submit to the [covid19-forecast-hub](https://github.com/reichlab/covid19-forecast-hub)

 To submit to the hub, first run
 ```
 git checkout master
 ```

 and then
 ```
 git checkout ludev [forecast_date]_LUcompUncertLab-VAR3streams.csv
 ```
 where forecast_date is the Monday of the submission date, in the format YYYY-MM-DD. This pulls the final submission file from the ludev branch into the master branch

 Finally, navigate to [our master branch on github](https://github.com/computationalUncertaintyLab/covid19-forecast-hub/tree/master/data-processed/LUcompUncertLab-VAR_3streams) and submit a pull request with the team name ```LUcompUncertLab``` and the model name (just to be safe) ```VAR3streams``` in the title. 
 
 The pull request should then go through auto-validation. If you would like to make changes because it failed validation or for other reasons, just push them to the [master branch](https://github.com/computationalUncertaintyLab/covid19-forecast-hub) and the validation will rerun, as long as the pull request remains open.