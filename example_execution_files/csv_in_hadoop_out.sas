/*=========================================================================================*/
/*  Copyright © 2020, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.             */
/*  SPDX-License-Identifier: Apache-2.0                                                    */
/*=========================================================================================*/


/**************************************************************************/
/* CAS statement to start CAS session.                                    */
/**************************************************************************/

cas casl sessopts=(timeout=99 locale="en_US");


/**************************************************************************/
/* CASLIB statement for input_caslib.                                     */
/**************************************************************************/

caslib mySrclib 
  datasource=(srctype='dnfs') 
  path="/tmp/AIot/params/parameter_file.txt"


/**************************************************************************/
/* CASLIB statement for backup_caslib.                                    */
/**************************************************************************/

caslib myHdplib 
  datasource=(
    srctype="hadoop" 
    server="myserver"
    hadoopjarpath="/home/dbclients/hadoop/cloudera/clusters/DIP/lib"
    hadoopconfigdir="/home/dbclients/hadoop/cloudera/clusters/DIP/conf"
    authDomain=HadoopAuth
    schema=aiot) 
  notactive;


/**************************************************************************/
/* Macro call.                                                            */
/**************************************************************************/

/*=========================================================================================*/
/* Execute the macro.                                                                      */
/*                                                                                         */
/* Parameters:                                                                             */
/*                                                                                         */
/* - parameter_file    : Fully qualified name the file containing the parameters processed */
/*                       at run-time. Ignored if load_pregen is set to Y.                  */
/* - print_parameters  : Y/N. Whether to print parameters and their values in the log. For */
/*                       debug purposes. Ignored if load_pregen is set to Y.               */
/* - first_step        : Start/restart step. Ignored if load_pregen is set to Y.           */
/* - input_caslib      : The caslib pointing to the location of the input data.            */
/*                       If load_pregen is set to Y, this option points to the location of */
/*                       a set of pre-generated tables.                                    */
/* - backup_caslib     : Optional. It specifies the caslib used to archive the tables.     */
/*                       Ignored if load_pregen is set to Y.                               */
/* - replace_backup    : Y/N, optional. It specifies whether to replace output tables when */
/*                       archiving them. Default is N. Ignored if load_pregen is set to Y. */
/* - load_pregen       : Y/N, optional. It specifies whether to load pre-generated tables  */
/*                       saved on a previous run.                                          */
/* - debug             : Y/N. Whether to generate debug info during execution, and whether */
/*                       to remove work datasets.                                          */
/*=========================================================================================*/

%aiot_main(
  parameter_file=/tmp/AIot/params/parameter_file.txt, 
  print_parameters=y, 
  first_step=1, 
  input_caslib=mySrclib, 
  backup_caslib= myHdplib, 
  replace_backup=Y, 
  load_pregen=, 
  debug=n );
