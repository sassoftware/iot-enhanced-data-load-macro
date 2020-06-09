# SAS Analytics for IoT - Alternate ETL Process

The SAS Analytics for IoT Enhanced Data Load Macro is an alternate tool that can be used to load the data mart for the SAS Analytics for IoT application. It provides additional functionality and can be used to load the data mart when the traditional data load macro does not meet your needs. You should already be familiar with the production ETL process.

## Overview

The following section describes the additional functionality supported by the enhanced data load macro.

### SAS Viya Data Sources

The enhanced data load macro supports any type of data source supported by SAS Viya including:

* SAS datasets
* Database tables
* SAS Event Stream Processing data
* CSV Files

### Datetime Value Precision

The precision of datetime values is maintained regardless of the granularity.

### Macro Restart Functionality

Functionality is included to restart the enhanced data load macro from any step if it does not complete successfully. This means you do not have to restart the macro from the beginning if a data load fails. This can be especially useful on large data loads.

### Filter Input Data Source

The enhanced data load macro supports filtering the input data at the data source level. This can be useful for large data sources where the entire source is not needed for analysis.

### Executed Entirely In CAS

To take advantage of the power of Cloud Analytic Services (CAS), the entire enhanced data load macro executes using only CAS. This improves performance in most cases.

### Save Analytics for IoT Data Mart Tables

Because the SAS Analytics for IoT data mart is stored using CAS, it resides entirely in memory. Therefore, if the memory becomes unloaded for any reason you must reload the data mart. The enhanced data load macro allows you to save the data mart tables to a permanent location. The tables can then be easily reloaded and the Analytics for IoT data mart will be restored.

### Code Written in CASL

To leverage the performance and functionality of CAS, the enhanced data load macro is written almost entirely in the Cloud Analytic Services Language (CASL).

## Process Description

The enhanced data load macro process is very similar to the production data load process. A parameter file continues to be used to control the process and the SAS Analytics for IoT data mart is loaded upon successful completion of the process.

### Macro Steps

The enhanced data load macro is divided into individual steps each acting independently. This allows you to restart the macro from any of the steps.

The following table lists the steps and provide a description of each for the enhanced data load macro:

| Step | Description |
| ------ | ------ |
| Step 1 | Read the parameter file and load input tables to a temporary location |
| Step 2 | Change the format of variable DATETIME in the measures table to improve precision | 
| Step 3 | Create PAM_ASSET_DIM table in data mart |
| Step 4 | Create PAM_TAG_DIM table in data mart | 
| Step 5 | Create PAM_ASSET_LOC_MEASURE_FACT table in data mart |
| Step 6 | Create PAM_FACT_TAG_MEASURES_STATS table in data mart | 
| Step 7 | Create PAM_EVENT_DIM table in data mart if required |
| Step 8 | Create PAM_ASSET_LOC_EVENT_FACT table in data mart if required | 
| Step 9 | Create PAM_FACT_EVENT_STATS table in data mart if required |
| Step 10 | Create hierarchies if required | 
| Step 11 | Create PREDEFINED_DATE table in data mart |
| Step 12 | Create copy of data mart tables and save to backup_caslib if specified | 
| Step 13 | Clean up process |

### Parameter File
The enhanced data load macro uses a parameter file to control the process. The following table lists the available parameters, whether they are required, and provides a brief description of each parameter:

| Parameter | Required? | Description |
| ------ | ------ | ------ |
| datetime_column | Yes | Name of column containing datetime values in measures file or table |
| device_id_column | Yes | Name of column containing DEVICE_ID values |
| event_aggreg_missing_method | Only if event measures loaded | Method used to aggregate missing values in event measures file or table. Default value is AVERAGE. |
| event_id_column | Only if event measures loaded | Name of column containing EVENT_ID values |
| measure_table | Yes | Path and name of measures file or table |
| event_table | No | Path and name of events file or table |
| event_measures_table | No | Path and name of event measures file or table |
| event_hierarchy_table | No | Path and name of event hierarchy file or table |
| device_table | No | Path and name of devices file or table |
| device_hierarchy_table | No | Path and name of devices hierarchy file or table |
| sensor_table | No | Path and name of sensors file or table |
| sensor_hierarchy_table | No | Path and name of sensors hierarchy file or table |
| measure_filter | No | SAS expression used to filter measures prior to being loaded |
| value_column | Yes | Name of column containing measurement values in measures file or table |
| missing_char_value | No | Character to use to represent missing values in the data mart. Default value is ?. |
| label_char_seperator | No | Character to replace the spaces in column names. | 
| sensor_aggreg_missing_method | Yes | Method used to aggregate missing values in measures file or table. Default value is AVERAGE. |
| sensor_id_column | Yes | Name of column containing SENSOR_ID values |
| sensor_interpol_missing_method | Yes | Method used to interpolate missing values in measures file or table. Default value is NONE. |
| node_column | No | Name of column containing node values in hierarchy files or tables. Default value is NODE. |
| parent_column | No | Name of column containing parent node values in hierarchy files or tables. Default value is PARENT. |
| level_column | No | Name of column containing level number values in hierarchy files or tables. Default value is LEVEL_NUM. |

## Prequisites

*  SAS Analytics for IoT
*  SAS Studio V
*  Knowledge of the production ETL process

## Running

There are several steps you need to perform to execute the enhanced data load macro:

*	Load the macro source code
*	Create a program file (.sas) that includes the following:
    o	Statement to create a CAS session
    o	Statement to assign a caslib for the imput data source
    o	Statement to assign a caslib for the output data if used
    o	Statement to call the macro with appropriate parameters
*	Execute the program file you created

### Load Macro Source Code

You must load the macro source code before you can execute the macro. You can do this by opening file AIoT_CASL.sas in SAS Studio V and submitting it.

### Create Program File

Create a new program file (.sas) in SAS Studio V or open one of the example files. The program file to execute the enhanced data load macro must include the following statements:

* Create a CAS session
* Assign a caslib for the imput data source
* Assign a caslib for the output data if used
* Call the macro with appropriate input parameters

#### Create a CAS Session

You must start a CAS session for the macro to execute. The example code includes the following statement that you can use:

```
cas casl sessopts=(timeout=99 locale="en_US");
```

#### CASLIB References

At a minimum, you will need a caslib for your input data source (input_caslib). If you are saving a copy of the data mart, you will also need a caslib for the output (backup_caslib).

A CAS session can have only one active caslib. A caslib becomes active when it is the first caslib assigned and no other caslibs are assigned, or they are assigned with the notactive option.

Ensure you assign the caslib referenced by input_caslib as the active caslib. If you are using the backup_caslib option, ensure you use the notactive option. The following example assigns the input_caslib as the active caslib and the backup_caslib as the notactive caslib:

```
caslib mySrclib 
  datasource=(srctype='dnfs') 
  path="/tmp/AIot/params/parameter_file.txt"

caslib myHdplib 
  datasource=(
    srctype="hadoop" 
    server="myserver" 
    hadoopjarpath="/home/dbclients/hadoop/cloudera/clusters/DIP/lib"
    hadoopconfigdir="/home/dbclients/hadoop/cloudera/clusters/DIP/conf"
    authDomain=HadoopAuth
    schema=aiot)
  notactive;
```

#### Macro Call

The macro call for the enhanced data load macro is as follows:

```
%aiot_main(
  parameter_file= , 
  print_parameters= , 
  first_step= , 
  input_caslib= , 
  backup_caslib= , 
  replace_backup=  , 
  load_pregen= , 
  debug=  );
```

##### Input Parameters

| Parameter | Description |
| ------ | ------ |
| parameter_file | Fully qualified name of parameter file |
| print_parameters | Whether or not to print the parameter file parameters and their values in the SAS Log. Valid values are Y and N. | 
| first_step | Process step where macro is to begin. Use 1 for normal operations. Valid values are 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, and 13. |
| input_caslib | The caslib library reference of the input data. If load_pregen is set to Y, the input_caslib parameter should reference the location of a set of previously saved tables. |
| backup_caslib | The caslib library reference of the location to where a copy of the data mart tables will be saved. |
| replace_backup | Whether or not the copy of the data mart tables being saved should be replaced. Valid values are Y and N. Default value is N. |
| load_pregen | Specifies to load a previously saved set of data mart tables should be used as input. Valid values are Y and N. If load_pregen is set to Y, the only other required parameter is input_caslib. All other parameters are ignored. |
| debug | Generates additional information in the SAS Log. Valid values are Y and N. |

## Examples

There are several example program files using various methods for executing the macro located in the [Example Execution Files](Example Execution Files) directory.

## Contributing

> We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit contributions to this project. 

## License

> This project is licensed under the [Apache 2.0 License](LICENSE).


