/*=========================================================================================*/
/*  Copyright © 2020, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.             */
/*  SPDX-License-Identifier: Apache-2.0                                                    */
/*=========================================================================================*/


%macro aiot_read_parameters;
       /*=========================================================================================*/
       /* Read parameter file and variables, unless we want to load a previously generated set of */
       /* tables.                                                                                 */
       /*=========================================================================================*/
       %global adj_measure_table measure_table adj_event_measure_table event_measure_table;
       %global adj_event_table event_table adj_event_hierarchy_table event_hierarchy_table;
       %global adj_device_table device_table adj_device_hierarchy_table device_hierarchy_table;
       %global adj_sensor_table sensor_table adj_sensor_hierarchy_table sensor_hierarchy_table;
       %global event_id_column device_id_column sensor_id_column value_column datetime_column;
       %global event_aggreg_missing_method sensor_aggreg_missing_method sensor_interpol_missing_method missing_char_val measure_filter;
       %global l_event_aggreg_missing_method l_sensor_aggreg_missing_method l_sensor_interpol_missing_method;
       %global node_column parent_column level_column label_char_separator aiot_caslib sasmsg_dataset cas_sasmsg_dataset;

       %let aiot_caslib=%str(QASMartStore);

       %if (&load_pre_gen = Y)
           %then %return;

       %if not (%sysfunc(fileexist(&param_file)))
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - ERROR: The parameter file &param_file does not exist or cannot be accessed.;           
                    %let return_code = 1;
                    %return;
                 %end;

       %let adj_measure_table = %str();
       %let measure_table = %str();
       %let adj_event_measure_table = %str();
       %let event_measure_table = %str();
       %let adj_device_table = %str();
       %let device_table = %str();
       %let adj_device_hierarchy_table = %str();
       %let device_hierarchy_table = %str();
       %let adj_event_table = %str();
       %let event_table = %str();
       %let adj_event_hierarchy_table = %str();
       %let event_hierarchy_table = %str();
       %let adj_sensor_table = %str();
       %let sensor_table = %str();
       %let adj_sensor_hierarchy_table = %str();
       %let sensor_hierarchy_table = %str();
       %let event_id_column = %str();
       %let device_id_column = %str();
       %let sensor_id_column = %str();
       %let value_column = %str();
       %let datetime_column = %str();
       %let node_column = %str(NODE);
       %let parent_column = %str(PARENT);
       %let level_column = %str(LEVEL_NUM);
       %let event_aggreg_missing_method = %str(AVERAGE);
       %let sensor_aggreg_missing_method = %str(AVERAGE);
       %let sensor_interpol_missing_method = %str(NONE);
       %let missing_char_val = %str(?);
       %let label_char_separator = %str();

       data _null_;
            infile "&param_file" encoding='utf-8' pad; 
            input row $256.;
            if (row ne '')
               then do;
                      param = upcase(kscan(row, 1, '='));
                      select (param);
                             when ('DATETIME_COLUMN')                call symputx('DATETIME_COLUMN', kscan(row, 2, '='));
                             when ('DEVICE_ID_COLUMN')               call symputx('DEVICE_ID_COLUMN', kscan(row, 2, '='));
                             when ('EVENT_AGGREG_MISSING_METHOD')    call symputx('EVENT_AGGREG_MISSING_METHOD', kscan(row, 2, '='));
                             when ('EVENT_ID_COLUMN')                call symputx('EVENT_ID_COLUMN', kscan(row, 2, '='));
                             when ('MEASURE_TABLE')                  call symputx('MEASURE_TABLE', kscan(row, 2, '='));
                             when ('EVENT_TABLE')                    call symputx('EVENT_TABLE', kscan(row, 2, '='));
                             when ('EVENT_MEASURE_TABLE')            call symputx('EVENT_MEASURE_TABLE', kscan(row, 2, '='));
                             when ('EVENT_HIERARCHY_TABLE')          call symputx('EVENT_HIERARCHY_TABLE', kscan(row, 2, '='));
                             when ('DEVICE_TABLE')                   call symputx('DEVICE_TABLE', kscan(row, 2, '='));
                             when ('DEVICE_HIERARCHY_TABLE')         call symputx('DEVICE_HIERARCHY_TABLE', kscan(row, 2, '='));
                             when ('SENSOR_TABLE')                   call symputx('SENSOR_TABLE', kscan(row, 2, '='));
                             when ('SENSOR_HIERARCHY_TABLE')         call symputx('SENSOR_HIERARCHY_TABLE', kscan(row, 2, '='));
                             when ('MEASURE_FILTER')                 call symputx('MEASURE_FILTER', ksubstr(row, kindex(row, '=') + 1));
                             when ('VALUE_COLUMN')                   call symputx('VALUE_COLUMN', kscan(row, 2, '='));
                             when ('MISSING_CHAR_VAL')               call symputx('MISSING_CHAR_VAL', kscan(row, 2, '='));   
                             when ('LABEL_CHAR_SEPARATOR')           call symputx('LABEL_CHAR_SEPARATOR', kscan(row, 2, '='));
                             when ('SENSOR_AGGREG_MISSING_METHOD')   call symputx('SENSOR_AGGREG_MISSING_METHOD', kscan(row, 2, '='));
                             when ('SENSOR_ID_COLUMN')               call symputx('SENSOR_ID_COLUMN', kscan(row, 2, '='));
                             when ('SENSOR_INTERPOL_MISSING_METHOD') call symputx('SENSOR_INTERPOL_MISSING_METHOD', kscan(row, 2, '='));
                             when ('NODE_COLUMN')                    call symputx('NODE_COLUMN', kscan(row, 2, '='));
                             when ('PARENT_COLUMN')                  call symputx('PARENT_COLUMN', kscan(row, 2, '='));
                             when ('LEVEL_COLUMN')                   call symputx('LEVEL_COLUMN', kscan(row, 2, '='));
                             otherwise do;
                                         put "ERROR: Unknown parameter " param "found in parameter file. Execution will halt.";
                                         call symputx('return_code', 1);
                                       end;
                    end;
            end;
       run;

       %if (&return_code eq 1)
           %then %return;

       %if (&print_params eq Y)
           %then %do;
                    %put Parameter list:;
                    %put - DATETIME_COLUMN                  &DATETIME_COLUMN;
                    %put - DEVICE_ID_COLUMN                 &DEVICE_ID_COLUMN;
                    %put - EVENT_AGGREG_MISSING_METHOD      &EVENT_AGGREG_MISSING_METHOD;
                    %put - EVENT_ID_COLUMN                  &EVENT_ID_COLUMN;
                    %put - MEASURE_TABLE                    &MEASURE_TABLE; 
                    %put - EVENT_TABLE                      &EVENT_TABLE;
                    %put - EVENT_MEASURE_TABLE              &EVENT_MEASURE_TABLE;
                    %put - EVENT_HIERARCHY_TABLE            &EVENT_HIERARCHY_TABLE;
                    %put - DEVICE_TABLE                     &DEVICE_TABLE;
                    %put - DEVICE_HIERARCHY_TABLE           &DEVICE_HIERARCHY_TABLE;
                    %put - SENSOR_TABLE                     &SENSOR_TABLE; 
                    %put - SENSOR_HIERARCHY_TABLE           &SENSOR_HIERARCHY_TABLE;
                    %put - MEASURE_FILTER                   &MEASURE_FILTER;
                    %put - VALUE_COLUMN                     &VALUE_COLUMN;
                    %put - MISSING_CHAR_VAL                 &MISSING_CHAR_VAL;
                    %put - LABEL_CHAR_SEPARATOR             &LABEL_CHAR_SEPARATOR;
                    %put - SENSOR_AGGREG_MISSING_METHOD     &SENSOR_AGGREG_MISSING_METHOD;
                    %put - SENSOR_ID_COLUMN                 &SENSOR_ID_COLUMN;
                    %put - SENSOR_INTERPOL_MISSING_METHOD   &SENSOR_INTERPOL_MISSING_METHOD;
                    %put - NODE_COLUMN                      &NODE_COLUMN;
                    %put - PARENT_COLUMN                    &PARENT_COLUMN;
                    %put - LEVEL_COLUMN                     &LEVEL_COLUMN;
                    %put --------------------------------------------------------------------------------------------------------------;
                 %end;

       %let adj_measure_table=%scan(&measure_table,1,".");
       %let adj_event_table=%scan(&event_table,1,".");
       %let adj_event_measure_table=%scan(&event_measure_table,1,".");
       %let adj_event_hierarchy_table=%scan(&event_hierarchy_table,1,".");
       %let adj_device_table=%scan(&device_table,1,".");
       %let adj_device_hierarchy_table=%scan(&device_hierarchy_table,1,".");
       %let adj_sensor_table=%scan(&sensor_table,1,".");
       %let adj_sensor_hierarchy_table=%scan(&sensor_hierarchy_table,1,".");
       %let l_event_aggreg_missing_method=%length(&event_aggreg_missing_method);
       %let l_sensor_aggreg_missing_method=%length(&sensor_aggreg_missing_method);
       %let l_sensor_interpol_missing_method=%length(&sensor_interpol_missing_method);
       %let sasmsg_dataset=%str(SASHELP.WRTYANL_AFI_CONFIG);
       %let cas_sasmsg_dataset=%str(CASUSER.WRTYANL_AFI_CONFIG);
%mend aiot_read_parameters;

%macro aiot_set_up_environment;
       /*=========================================================================================*/
       /* Set up environment                                                                      */
       /*=========================================================================================*/
       proc cas;
            session "&_sessref_";

            /*=========================================================================================*/
            /* Create the action set that groups together all UDF's and redefines them as CAS actions  */
            /*=========================================================================================*/
            builtins.defineActionSet result=res status=rc / 
                     name="AIot"
                     actions={
                              /*=========================================================================================*/
                              /* Output current time                                                                     */
                              /*=========================================================================================*/
                              {name="CurrentTime"
                               desc="Outputs the current time and a message"
                               parms={
                                      {name="message" type="string" required=TRUE}
                                     }
                               definition="print (put(date(),date9.)||"" ""||put(time(),tod8.)) message;
                                           send_response();"
                              },
                              /*=========================================================================================*/
                              /* Process return code from CAS action                                                     */
                              /*=========================================================================================*/
                              {name="ReturnCodeHandler"
                               desc="Handles the return code from a CAS action"
                               parms={
                                      {
                                       name="ret_code" type="paramlist" required=TRUE 
                                            subparms={
                           	                          {name="severity" type="int64"},
                                                      {name="reason" type="int64"},
                                                      {name="status" type="string"},
                                                      {name="statuscode" type="int64"}
                                                     }
                                      }
                                      {name="err_msg" type="string" required=TRUE}
                                      {name="ok_msg" type="string" required=TRUE}
                                     }
                               definition="code = ret_code;
                                           if (code.severity > 1)
                                              then do;
                                                     AIot.CurrentTime result=res / message=err_msg;
                                                     print 'Return code : ' code.severity;
                                                     print 'Reason code : ' code.reason;
                                                     print 'Status      : ' code.status;
                                                     print 'Status code : ' code.statusCode;
                                                     call symputx('return_code', 8);
                                                   end;
                                              else do;
                                                     AIot.CurrentTime result=res / message=ok_msg;
                                                     call symputx('return_code', 0);
                                                   end;
                                           send_response();"
                              },
                              /*=========================================================================================*/
                              /* Load input data into CAS                                                                */
                              /*=========================================================================================*/
                              {name="LoadInputData"
                               desc="Load input data into CAS"
                               parms={
                                      {name="input_caslib" type="string" required=TRUE}
                                      {name="input_caslib_type" type="string" required=TRUE}
                                      {name="output_caslib" type="string" required=TRUE}
                                      {name="long_table_name" type="string" required=FALSE notBlank=FALSE}
                                      {name="table_filter" type="string" required=FALSE notBlank=FALSE}
                                      {name="op_type" type="string" required=TRUE}
                                     }
                               definition="if (long_table_name == '')
                                              then exit;

                                           table_name = scan(long_table_name, 1, '.');

                                           if (table_filter = '')
                                              then table_filter = '0=0';

                                           /*=========================================================================================*/
                                           /* If loading from ESP, do some housekeeping:                                              */
                                           /* - Load metadata about the project;                                                      */
                                           /* - Determine the URI for the ESP window pointing to the input data;                      */
                                           /* - Load the input data;                                                                  */
                                           /* - Alter the target table to remove the opcode column, drop the asterisk from each of the*/
                                           /*   key columns, and upcase all columns.                                                  */
                                           /*=========================================================================================*/
                                           if (input_caslib_type = 'ESP')
                                              then do;
                                                     esp_Uri = '';

                                                     loadStreams.mMetaData result=res status=rc / 
                                                                 casLib=input_caslib;

                                                     MetaTab = findtable(res);

                                                     do col over MetaTab;
                                                        if (strip(col.window) == table_name)
                                                           then do;
                                                                  esp_Uri = strip(col.project)||'/'||strip(col.query)||'/'||strip(col.window);
                                                                  leave;
                                                                end;
                                                     end;

                                                     if (esp_Uri == '')
                                                        then do;
                                                               rc.severity = 2;
                                                               rc.status = '2720411';
                                                             end;
                                                        else do;
                                                               loadStreams.loadSnapshot result=res status=rc /
                                                                           casLib=input_caslib
                                                                           espUri=esp_Uri
                                                                           casOut={
                                                                                   caslib=output_caslib, 
                                                                                   name=table_name, 
                                                                                   replace=TRUE,
                                                                                   promote=FALSE
                                                                                   where=table_filter
                                                                                  };

                                                               table.columninfo result=res status=rc / 
                                                                     table={
                                                                            caslib=output_caslib
                                                                            name=table_name
                                                                           };

                                                               i=1;
                                                               alterList={};
   
                                                               ColInfo = findtable(res);

                                                               do col over ColInfo;
                                                                  if (upcase(col.column) = '_OPCODE') 
                                                                     then do;
                                                                            alterVar.name = col.column;
                                                                            alterVar.drop = true;
                                                                            alterList[i] = alterVar;
                                                                            i = i + 1;
                                                                          end;
                                                                     else do;
                                                                            alterVar.name = col.column;
                                                                            alterVar.drop = false;

                                                                            if (upcase(substr(col.column, length(col.column), 1)) == '*')
                                                                               then alterVar.rename = upcase(substr(col.column, 1, length(col.column) - 1));
                                                                               else alterVar.rename = upcase(col.column);

                                                                            if (
                                                                                (upcase(col.column) = ""&datetime_column"") or
                                                                                (upcase(col.column) = ""&datetime_column""||'*')
                                                                               )
                                                                               then alterVar.format = '';

                                                                            alterList[i]=alterVar;
                                                                            i = i + 1;
                                                                          end;
                                                               end;

                                                               table.altertable result=res status=rc / 
                                                                     caslib=output_caslib 
                                                                     name=table_name 
                                                                     columns=alterList;
                                                               rc.status = '';
                                                             end;
                                                   end;
                                              else do;
                                                     table.fileInfo result=res status=rc /
                                                           caslib=input_caslib
                                                           path=long_table_name;
                                                     rc.status = '';

                                                     if (
                                                         (dim(res.FileInfo) == 0) and
                                                         (op_type = 'LOAD_EXISTS')
                                                        )
                                                        then exit;

                                                     AIot.ReturnCodeHandler result=res / 
                                                           ret_code = rc
                                                           err_msg = "" Step &step - ERROR: Table or file ""||upcase(long_table_name)||"" cannot be found.""
                                                           ok_msg = "" Step &step - NOTE: Table or file ""||upcase(long_table_name)||"" found."";

                                                     table.loadtable result=res status=rc / 
                                                           caslib=input_caslib
                                                           path=long_table_name
                                                           casOut={
                                                                   caslib=output_caslib, 
                                                                   name=table_name, 
                                                                   replace=TRUE,
                                                                   promote=FALSE
                                                                  }
                                                           where=table_filter;
                                                     rc.status = '';
                                                   end;

                                           AIot.ReturnCodeHandler result=res / 
                                                ret_code = rc
                                                err_msg = "" Step &step - ERROR: An error occurred while loading the ""||upcase(table_name)||"" table.""
                                                ok_msg = "" Step &step - NOTE: Table ""||upcase(table_name)||"" loaded successfully."";

                                           send_response(rc);"
                              },
                              /*=========================================================================================*/
                              /* Check to see if the input table loaded into CAS is empty                                */
                              /*=========================================================================================*/
                              {name="IsEmpty"
                               desc="Check to see if the input table loaded into CAS is empty"
                               parms={
                                      {name="table_name" type="string" required=TRUE}
                                     }
                               definition="if (table_name == '')
                                              then exit;

                                           table.recordCount result=res status=rc / table={caslib=""&inp_caslib"" name=table_name};

                                           if (res.recordCount[1, 'N'] == 0)
                                              then do;
                                                     AIot.CurrentTime result=res / message = "" Step &step - NOTE: Table ""||upcase(table_name)||"" is empty."";
                                                     call symputx('return_code', 8);
                                                   end;

                                           send_response(rc);"
                              }
                             };

            /*=========================================================================================*/
            /* Query target CASLIB, where the tables will be generated                                 */
            /*=========================================================================================*/
            AIot.CurrentTime result=res / message = " NOTE: Querying caslib "||upcase("&inp_caslib")||"...";

            queryCaslib result=res status=rc /
                        caslib="&inp_caslib";

            if (res[1] = FALSE)
               then do;
                      AIot.CurrentTime result=res / message = " Step &step - ERROR: The CAS library "||upcase("&inp_caslib")||" is not assigned.";
                      call symputx('return_code', 8);
                      exit;
                    end;

            run;
       quit;
%mend aiot_set_up_environment;

%macro aiot_load_data;
       /*=========================================================================================*/
       /* Load the input data set(s)                                                              */
       /*=========================================================================================*/
       proc cas; 
            session "&_sessref_";

            table.caslibInfo result=input_caslib_info status=rc /
                  caslib="&inp_caslib";

            if ("&load_pre_gen" == 'Y')
               then do;
                      /*=========================================================================================*/
                      /* Load a pre-existing set of tables from the ones shown below                             */
                      /*=========================================================================================*/
                      TableList = {"FILTER_ATTRIBUTE",
                                   "FILTER_ATTRIBUTE_GROUP",
                                   "FILTER_ATTRIBUTE_TREE",
                                   "LOCALIZATION_GROUP_DATASELECTION",
                                   "LOCALIZATION_GROUP_MARTMETADATA",
                                   "LOCALIZATION_VALUE_DATASELECTION",
                                   "LOCALIZATION_VALUE_MARTMETADATA",
                                   "PAM_ASSET_DIM", 
                                   "PAM_ASSET_LOC_EVENT_FACT", 
                                   "PAM_ASSET_LOC_MEASURE_FACT", 
                                   "PAM_EVENT_DIM", 
                                   "PAM_FACT_EVENT_STATS", 
                                   "PAM_FACT_TAG_MEASURES_STATS", 
                                   "PAM_TAG_DIM", 
                                   "PREDEFINED_DATE",
                                   "TABLE_ATTRIBUTES",
                                   "TABLE_META",
                                   "TABLE_META_REFRESH_DATES",
                                   "TABLECOLUMN_ATTRIBUTES",
                                   "TABLECOLUMN_META"};

                      AIot.CurrentTime result=res / message = " Step &step - NOTE: Loading tables from "||upcase("&inp_caslib")||"...";

                      if (
                          (input_caslib_info.CASLibInfo[1].Type == 'DNFS') or
                          (input_caslib_info.CASLibInfo[1].Type == 'PATH')
                         )
                         then extension = '.sashdat';
                         else extension = '';

                      do i = 1 to dim(TableList);
                         AIot.LoadInputData result=rc / 
                              input_caslib="&inp_caslib"
                              input_caslib_type=input_caslib_info.CASLibInfo[1].Type
                              output_caslib="&inp_caslib"
                              long_table_name=TableList[i]||extension
                              table_filter=""
                              op_type="LOAD_EXISTS";
                      end;
                    end;
               else do;
                      /*=========================================================================================*/
                      /* Load input data into CAS                                                                */
                      /*=========================================================================================*/
                      TableList = {"&measure_table",
                                   "&event_table", 
                                   "&event_measure_table", 
                                   "&event_hierarchy_table", 
                                   "&device_table", 
                                   "&device_hierarchy_table", 
                                   "&sensor_table", 
                                   "&sensor_hierarchy_table"};

                      AIot.CurrentTime result=res / message = " Step &step - NOTE: Loading input data from "||upcase("&inp_caslib")||"...";

                      do i = 1 to dim(TableList);
                         if (i < 3)
                            then do;
                                   /*=========================================================================================*/
                                   /* Data filtering applies to the measure and event_measure table only                      */
                                   /*=========================================================================================*/
                                   AIot.LoadInputData result=rc / 
                                        input_caslib="&inp_caslib"
                                        input_caslib_type=input_caslib_info.CASLibInfo[1].Type
                                        output_caslib="&inp_caslib"
                                        long_table_name=TableList[i]
                                        table_filter="&measure_filter"
                                        op_type="LOAD_NEW";
                                 end;
                            else do;
                                   AIot.LoadInputData result=rc / 
                                        input_caslib="&inp_caslib"
                                        input_caslib_type=input_caslib_info.CASLibInfo[1].Type
                                        output_caslib="&inp_caslib"
                                        long_table_name=TableList[i]
                                        table_filter=""
                                        op_type="LOAD_NEW";
                                 end;

                        if (dim(rc) > 0)
                           then if (rc.severity > 1)
                                   then do;
                                          call symputx('return_code', 8);
                                          exit(0);
                                        end;

                         AIot.IsEmpty result=rc /
                              table_name=TableList[i];
                      end;
                    end;
            run;
       quit;
%mend aiot_load_data;

%macro aiot_datetime_type_conversion(table_name);
       /*=========================================================================================*/
       /* Change the datetime column type from char to timestamp                                  */
       /*=========================================================================================*/
       %if (&table_name eq)
           %then %return;

       proc cas; 
            session "&_sessref_";

            function is_conversion_needed(&table_name, column_name);
                     table.columninfo result=res / 
                           table="&table_name";

                     colinfo = findtable(res);
     
                     do col over colinfo;
                        if (upcase(col.Column) == upcase("&datetime_column")) 
                           then if (col.type = 'double')
                                   then return(0);
                                   else if (col.type = 'datetime')
                                           then return (1);
                     end;
                     return(2);
            end;

            need_conv = is_conversion_needed("&datetime_column");

            if (need_conv == 0)
               then do;
                      AIot.CurrentTime result=res / message = " Step &step - NOTE: Conversion not needed for column "||upcase("&datetime_column")||" in table "||upcase("&table_name")||"...";
                      exit;
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Changing datetime column type for column "||upcase("&datetime_column")||" in table "||upcase("&table_name")||"...";

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name="&table_name";
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the "||upcase("&table_name")||" table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the "||upcase("&table_name")||" table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            if (need_conv == 1)
               then do;
                      datastep.runcode result=res status=rc / 
                               code = "data &inp_caslib..&table_name;
                                            set &inp_caslib..&table_name;
                                       run;"
                               single="NO";
                      end;
               else do;
                      datastep.runcode result=res status=rc / 
                               code = "data &inp_caslib..&table_name (drop=&datetime_column rename=(&datetime_column"||"_TEMP"||"=&datetime_column));
                                            set &inp_caslib..&table_name;
                                            /* Do this to check for leading blanks */
                                            if (
                                                (kindex(kleft(&datetime_column), ' ') > 0) and
                                                (kindex(kleft(&datetime_column), ' ') < klength(kleft(&datetime_column)))
                                               )
                                               then &datetime_column"||"_TEMP"||"=input(kscan(kleft(&datetime_column),1,' '),anydtdtm.)+input(kscan(kleft(&datetime_column),2,' '),anydttme.);
		                                       else &datetime_column"||"_TEMP"||"=input(kleft(&datetime_column),datetime25.6);
                                       run;"
                               single="NO";
                    end;
            rc.status = '';

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while changing the column type for column "||upcase("&datetime_column")||" in table "||upcase("&table_name")||"..."
                 ok_msg = " Step &step - NOTE: Datetime column type for column "||upcase("&datetime_column")||" in table "||upcase("&table_name")||" changed successfully.";
            run;
       quit;
%mend aiot_datetime_type_conversion;

%macro aiot_pam_asset_dim;
       /*=========================================================================================*/
       /* Create PAM_ASSET_DIM                                                                    */
       /*=========================================================================================*/
       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the PAM_ASSET_DIM table...";

            /*=========================================================================================*/
            /* If a device column is provided but no device table has been specified, the column could */
            /* be in the measures table. If not, get out of dodge.                                     */
            /*=========================================================================================*/
            if ("&adj_device_table" == '')
               then do;
                      if ("&device_id_column" == '')
                         then do;
                                ds2.runDS2 result=res status=rc / 
                                    program = "data &inp_caslib..PAM_ASSET_DIM / overwrite=yes;
                                                    dcl integer      PAM_ASSET_DIM_RK;
                                                    dcl varchar(100) PAM_ASSET_ID;
                                                    dcl integer      CURRENT_NUM;
                                                    dcl double       PROCESS_DTTM having format datetime25.6;

                                               method run();
                                                      PAM_ASSET_DIM_RK = 0; 
                                                      PAM_ASSET_ID = '&missing_char_val'; 
                                                      CURRENT_NUM = 1; 
                                                      PROCESS_DTTM = %sysfunc(datetime()); 
                                                      output;
                                               end;
                                               enddata;";
                              end;
                         else do;
                                table.columninfo result=res status=rc / table={caslib="&inp_caslib", name="&adj_measure_table"};

	                            device_in_measures = FALSE;
                                do col over res.ColumnInfo;
                                   if (upcase(col.column) == upcase("&device_id_column"))
                                      then device_in_measures = TRUE;
                                end;

                                if (device_in_measures = FALSE)
                                   then do;
                                          AIot.CurrentTime result=res / message = " Step &step - ERROR: The "||upcase("&device_id_column")||" is neither in the device nor in the measure table.";
                                          call symputx('return_code', 8);
                                          exit;
                                        end;

                                ds2.runDS2 result=res status=rc / 
                                    program = "data &inp_caslib..PAM_ASSET_DIM (keep=(PAM_ASSET_DIM_RK PAM_ASSET_ID CURRENT_NUM PROCESS_DTTM)) / overwrite=yes;
                                                    dcl integer      PAM_ASSET_DIM_RK;
                                                    dcl varchar(100) PAM_ASSET_ID;
                                                    dcl integer      CURRENT_NUM;
                                                    dcl double       PROCESS_DTTM having format datetime25.6;

                                               method run();
                                                      set &inp_caslib..""&adj_measure_table"" (in=a);
                                                       by ""&device_id_column"";
                                                      if (first.""&device_id_column"")
                                                         then do;
                                                                PAM_ASSET_DIM_RK + 1; 
                                                                PAM_ASSET_ID = ""&device_id_column""; 
                                                                CURRENT_NUM = 1; 
                                                                PROCESS_DTTM = %sysfunc(datetime()); 
                                                                output;
                                                              end;
                                               end;
                                               enddata;";
                              end;
                    end;
               else do;
                      table.tableExists result=res status=rc /
                            caslib="&inp_caslib"
                            name="&adj_device_table";
                      rc.status = '';

                      if (res.exists == 0)
                         then rc.severity = 2;

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while accessing the "||upcase("&adj_device_table")||" table."
                           ok_msg = " Step &step - NOTE: Existence successfully verified for the "||upcase("&adj_device_table")||" table.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;

                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..PAM_ASSET_DIM (drop=(""&device_id_column"")) / overwrite=yes;
                                          dcl integer      PAM_ASSET_DIM_RK;
                                          dcl varchar(100) PAM_ASSET_ID;
                                          dcl integer      CURRENT_NUM;
                                          dcl double       PROCESS_DTTM having format datetime25.6;

                                     method run();
                                            set &inp_caslib..""&adj_device_table"" (in=a);
                                             by ""&device_id_column"";
                                            if (first.""&device_id_column"")
                                               then do;
                                                      PAM_ASSET_DIM_RK + 1; 
                                                      PAM_ASSET_ID = ""&device_id_column""; 
                                                      CURRENT_NUM = 1; 
                                                      PROCESS_DTTM = %sysfunc(datetime()); 
                                                      output;
                                                    end;
                                     end;
                                     enddata;";
                    end;
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating PAM_ASSET_DIM."
                 ok_msg = " Step &step - NOTE: Table PAM_ASSET_DIM created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;
%mend aiot_pam_asset_dim;

%macro aiot_pam_tag_dim;
       /*=========================================================================================*/
       /* Create PAM_TAG_DIM.                                                                     */
       /*=========================================================================================*/
       %if (&adj_measure_table eq)
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Skipping PAM_TAG_DIM creation...;
                    %return;
                 %end;

       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the PAM_TAG_DIM table...";

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name="&adj_measure_table";
            rc.status = '';
            
            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the "||upcase("&adj_measure_table")||" table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the "||upcase("&adj_measure_table")||" table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            if ("&adj_sensor_table" == '')
               then do;
                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..PAM_TAG_DIM (drop=(curr_tstamp)) / overwrite=yes;
                                          dcl integer      PAM_TAG_DIM_RK CURRENT_NUM;
                                          dcl char(256)    TAG_ID;
                                          dcl varchar(256) TAG_COLUMN_NM;
                                          dcl double       PROCESS_DTTM having format datetime25.6;
                                          dcl char(&l_sensor_aggreg_missing_method) TAG_DEF_AGGREG_METHOD_CD;
                                          dcl char(&l_sensor_interpol_missing_method) TAG_DEF_INTERPOL_METHOD_CD;
                                          dcl double       curr_tstamp;
                                          retain curr_tstamp;

                                          method init();
                                                 PAM_TAG_DIM_RK = 1;
                                                 curr_tstamp = datetime();
                                          end;
                                          method run();
                                                 set &inp_caslib..""&adj_measure_table"" (keep=(""&sensor_id_column"") rename=(""&sensor_id_column"" as TAG_ID));
                                                  by TAG_ID;
                                                 if (first.TAG_ID) 
                                                    then do;
                                                           TAG_COLUMN_NM = TAG_ID;
                                                           TAG_DEF_INTERPOL_METHOD_CD = '&sensor_interpol_missing_method';
                                                           TAG_DEF_AGGREG_METHOD_CD = '&sensor_aggreg_missing_method';
                                                           CURRENT_NUM = 1;
                                                           PROCESS_DTTM = curr_tstamp;
                                                           output;
                                                           PAM_TAG_DIM_RK + 1;
                                                         end;
                                          end;
                                     enddata;";
                    end;
               else do;
                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..PAM_TAG_DIM (drop=(curr_tstamp ""&device_id_column"" &datetime_column ""&value_column"" PAM_TAG_ID) rename=(""&sensor_id_column""=TAG_ID ""&value_column""=LOC_MEASURE_VALUE)) / overwrite=yes;
                                          dcl integer      PAM_TAG_DIM_RK CURRENT_NUM;
                                          dcl varchar(256) TAG_COLUMN_NM;
                                          dcl char(&l_sensor_aggreg_missing_method) TAG_DEF_AGGREG_METHOD_CD;
                                          dcl char(&l_sensor_interpol_missing_method) TAG_DEF_INTERPOL_METHOD_CD;
                                          dcl double       PROCESS_DTTM having format datetime25.6;
                                          dcl double       curr_tstamp;
                                          retain curr_tstamp;

                                          method init();
                                                 PAM_TAG_DIM_RK = 0;
                                                 curr_tstamp = datetime();
                                          end;
                                          method run();
                                                 merge &inp_caslib..""&adj_measure_table"" (in=a)
                                                       &inp_caslib..""&adj_sensor_table"" (in=b);
                                                    by ""&sensor_id_column"";
                                                 if (b and first.""&sensor_id_column"")
                                                    then do;
                                                           PAM_TAG_DIM_RK + 1;
                                                           TAG_COLUMN_NM = ""&sensor_id_column"";
                                                           TAG_DEF_INTERPOL_METHOD_CD = '&sensor_interpol_missing_method';
                                                           TAG_DEF_AGGREG_METHOD_CD = '&sensor_aggreg_missing_method';
                                                           CURRENT_NUM = 1;
                                                           PROCESS_DTTM = curr_tstamp;
                                                           output;
                                                         end;
                                          end;
                                     enddata;";
                    end;
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating PAM_TAG_DIM."
                 ok_msg = " Step &step - NOTE: Table PAM_TAG_DIM created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;
%mend aiot_pam_tag_dim;

%macro aiot_pam_asset_loc_measure_fact;
       /*=========================================================================================*/
       /* Create PAM_ASSET_LOC_MEASURE_FACT                                                       */
       /*=========================================================================================*/
       %if (&adj_measure_table eq)
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Skipping PAM_ASSET_LOC_MEASURE_FACT creation...;
                    %return;
                 %end;

       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the PAM_ASSET_LOC_MEASURE_FACT table...";

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name="&adj_measure_table";
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the "||upcase("&adj_measure_table")||" table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the "||upcase("&adj_measure_table")||" table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            table.columninfo result=res status=rc / table={caslib="&inp_caslib", name="&adj_measure_table"};

            AdditionalCols = '';
            is_first = TRUE;
            do col over res.ColumnInfo;
               if (
                   (upcase(col.column) != upcase("&sensor_id_column")) and
                   (upcase(col.column) != upcase("&value_column")) and
                   (upcase(col.column) != upcase("&datetime_column")) and
                   (upcase(col.column) != upcase("&device_id_column"))
                  )
                  then do;
                         if (is_first)
                            then do;
                                   AdditionalCols = ", a."||col.column;
                                   is_first = FALSE;
                                 end;
                            else AdditionalCols = AdditionalCols||", a."||col.column;
                       end;
            end;

            if ("&device_id_column" != '')
               then do;
                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..PAM_ASSET_LOC_MEASURE_FACT (drop=(curr_tstamp &datetime_column ""&device_id_column"")) / overwrite=yes;
                                          dcl double    PAM_TAG_DIM_RK LOC_MEASURE_VALUE PAM_ASSET_DIM_RK PAM_DATE_DIM_RK PAM_TIME_DIM_RK;
                                          dcl double    MEASURE_DTTM having format datetime25.6;
                                          dcl double    PROCESS_DTTM having format datetime25.6;
                                          dcl double    curr_tstamp;
                                          retain curr_tstamp;

                                          method init();
                                                 curr_tstamp = datetime();
                                          end;
                                          method run();
                                                 set {select b.PAM_ASSET_DIM_RK,
                                                             c.PAM_TAG_DIM_RK,
                                                             a.""&value_column"" as LOC_MEASURE_VALUE,
                                                             a.&datetime_column as MEASURE_DTTM "||AdditionalCols||"
                                                        from &inp_caslib..""&adj_measure_table"" a,
                                                             &inp_caslib..PAM_ASSET_DIM b,
                                                             &inp_caslib..PAM_TAG_DIM c
                                                       where a.""&sensor_id_column"" = c.TAG_ID
                                                         and a.""&device_id_column"" = b.PAM_ASSET_ID};
                                                 PAM_DATE_DIM_RK = datepart(MEASURE_DTTM);
                                                 PAM_TIME_DIM_RK = timepart(MEASURE_DTTM);
                                                 PROCESS_DTTM = curr_tstamp;
                                          end;
                                     enddata;";
                    end;
               else do;
                      /* PAM_ASSET_DIM_RK should be 0 */
                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..PAM_ASSET_LOC_MEASURE_FACT (drop=(curr_tstamp &datetime_column ""&device_id_column"")) / overwrite=yes;
                                          dcl double    PAM_TAG_DIM_RK LOC_MEASURE_VALUE PAM_ASSET_DIM_RK PAM_DATE_DIM_RK PAM_TIME_DIM_RK;
                                          dcl double    MEASURE_DTTM having format datetime25.6;
                                          dcl double    PROCESS_DTTM having format datetime25.6;
                                          dcl double    curr_tstamp;
                                          retain curr_tstamp;

                                          method init();
                                                 curr_tstamp = datetime();
                                          end;
                                          method run();
                                                 set {select b.PAM_ASSET_DIM_RK, 
                                                             c.PAM_TAG_DIM_RK,
                                                             a.""&value_column"" as LOC_MEASURE_VALUE,
                                                             a.&datetime_column as MEASURE_DTTM "||AdditionalCols||"
                                                        from &inp_caslib..""&adj_measure_table"" a,
                                                             &inp_caslib..PAM_ASSET_DIM b,
                                                             &inp_caslib..PAM_TAG_DIM c
                                                       where a.""&sensor_id_column"" = c.TAG_ID};
                                                 PAM_DATE_DIM_RK = datepart(MEASURE_DTTM);
                                                 PAM_TIME_DIM_RK = timepart(MEASURE_DTTM);
                                                 PROCESS_DTTM = curr_tstamp;
                                          end;
                                     enddata;";
                    end;
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating PAM_ASSET_LOC_MEASURE_FACT."
                 ok_msg = " Step &step - NOTE: Table PAM_ASSET_LOC_MEASURE_FACT created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;
%mend aiot_pam_asset_loc_measure_fact;

%macro aiot_predefined_date;
       /*=========================================================================================*/
       /* Create PREDEFINED_DATE                                                                  */
       /*=========================================================================================*/
       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the PREDEFINED_DATE table...";

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..PREDEFINED_DATE / overwrite=yes;
                                dcl char(10)     DATE_PERIOD_CD;
                                dcl varchar(500) DATE_PERIOD_MACRO_CALL_TEXT;
                                dcl varchar(128) DATE_PERIOD_LABEL;
                                dcl int          DISPLAY_ORDER;

                           method run();
                                  DATE_PERIOD_CD = '7'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=7))'; DATE_PERIOD_LABEL = 'Last 7 Days'; DISPLAY_ORDER = 1; output;
                                  DATE_PERIOD_CD = '30'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=30))'; DATE_PERIOD_LABEL = 'Last 30 Days'; DISPLAY_ORDER = 2; output;
                                  DATE_PERIOD_CD = '90'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=90))'; DATE_PERIOD_LABEL = 'Last 90 Days'; DISPLAY_ORDER = 3; output;
                                  DATE_PERIOD_CD = '180'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=180))'; DATE_PERIOD_LABEL = 'Last 180 Days'; DISPLAY_ORDER = 4; output;
                                  DATE_PERIOD_CD = 'L2Y'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=L2Y))'; DATE_PERIOD_LABEL = 'Last 2 Years'; DISPLAY_ORDER = 5; output;
                                  DATE_PERIOD_CD = 'L3Y'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=L3Y))'; DATE_PERIOD_LABEL = 'Last 3 Years'; DISPLAY_ORDER = 6; output;
                                  DATE_PERIOD_CD = 'L4Y'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=L4Y))'; DATE_PERIOD_LABEL = 'Last 4 Years'; DISPLAY_ORDER = 7; output;
                                  DATE_PERIOD_CD = 'L5Y'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=L5Y))'; DATE_PERIOD_LABEL = 'Last 5 Years'; DISPLAY_ORDER = 8; output;
                                  DATE_PERIOD_CD = 'L6Y'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=L6Y))'; DATE_PERIOD_LABEL = 'Last 6 Years'; DISPLAY_ORDER = 9; output;
                                  DATE_PERIOD_CD = 'LY'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=LY))'; DATE_PERIOD_LABEL = 'Last Year'; DISPLAY_ORDER = 10; output;
                                  DATE_PERIOD_CD = 'LYP12CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=LYP12CM))'; DATE_PERIOD_LABEL = 'Last year, previous 12 calendar months'; DISPLAY_ORDER = 11; output;
                                  DATE_PERIOD_CD = 'LYP3CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=LYP3CM))'; DATE_PERIOD_LABEL = 'Last year, previous 3 calendar months'; DISPLAY_ORDER = 12; output;
                                  DATE_PERIOD_CD = 'LYPCM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=LYPCM))'; DATE_PERIOD_LABEL = 'Last year, previous calendar month'; DISPLAY_ORDER = 13; output;
                                  DATE_PERIOD_CD = 'LYTD'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=LYTD))'; DATE_PERIOD_LABEL = 'Last year to date'; DISPLAY_ORDER = 14; output;
                                  DATE_PERIOD_CD = 'LYTDPCM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=LYTDPCM))'; DATE_PERIOD_LABEL = 'Last year to previous calendar month'; DISPLAY_ORDER = 15; output;
                                  DATE_PERIOD_CD = 'P12CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P12CM))'; DATE_PERIOD_LABEL = 'Previous 12 calendar months'; DISPLAY_ORDER = 16; output;
                                  DATE_PERIOD_CD = 'P3CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P3CM))'; DATE_PERIOD_LABEL = 'Previous 3 calendar months'; DISPLAY_ORDER = 17; output;
                                  DATE_PERIOD_CD = 'PCM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=PCM))'; DATE_PERIOD_LABEL = 'Previous Calendar Month'; DISPLAY_ORDER = 18; output;
                                  DATE_PERIOD_CD = 'PCQ'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=PCQ))'; DATE_PERIOD_LABEL = 'Previous Calendar Quarter'; DISPLAY_ORDER = 19; output;
                                  DATE_PERIOD_CD = 'PCW'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=PCW))'; DATE_PERIOD_LABEL = 'Previous Calendar Week'; DISPLAY_ORDER = 20; output;
                                  DATE_PERIOD_CD = 'PCY'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=PCY))'; DATE_PERIOD_LABEL = 'Previous Calendar Year'; DISPLAY_ORDER = 21; output;
                                  DATE_PERIOD_CD = 'YTD'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=YTD))'; DATE_PERIOD_LABEL = 'Year to Date'; DISPLAY_ORDER = 22; output;
                                  DATE_PERIOD_CD = 'YTDPCM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=YTDPCM))'; DATE_PERIOD_LABEL = 'Year to previous calendar month'; DISPLAY_ORDER = 23; output;
                                  DATE_PERIOD_CD = 'P24CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P24CM))'; DATE_PERIOD_LABEL = 'Previous 24 calendar months'; DISPLAY_ORDER = 24; output;
                                  DATE_PERIOD_CD = 'P36CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P36CM))'; DATE_PERIOD_LABEL = 'Previous 36 calendar months'; DISPLAY_ORDER = 25; output;
                                  DATE_PERIOD_CD = 'P48CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P48CM))'; DATE_PERIOD_LABEL = 'Previous 48 calendar months'; DISPLAY_ORDER = 26; output;
                                  DATE_PERIOD_CD = 'P60CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P60CM))'; DATE_PERIOD_LABEL = 'Previous 60 calendar months'; DISPLAY_ORDER = 27; output;
                                  DATE_PERIOD_CD = 'P72CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P72CM))'; DATE_PERIOD_LABEL = 'Previous 72 calendar months'; DISPLAY_ORDER = 28; output;
                                  DATE_PERIOD_CD = 'P84CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P84CM))'; DATE_PERIOD_LABEL = 'Previous 84 calendar months'; DISPLAY_ORDER = 29; output;
                                  DATE_PERIOD_CD = 'P90CM'; DATE_PERIOD_MACRO_CALL_TEXT = '%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=P90CM))'; DATE_PERIOD_LABEL = 'Previous 90 calendar months'; DISPLAY_ORDER = 30; output;
                           end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating PREDEFINED_DATE."
                 ok_msg = " Step &step - NOTE: Table PREDEFINED_DATE created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;
%mend aiot_predefined_date;

%macro aiot_pam_fact_tag_measures_stat;
       /*=========================================================================================*/
       /* Create PAM_FACT_TAG_MEASURES_STATS                                                      */
       /*=========================================================================================*/
       %if (&adj_measure_table eq)
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Skipping PAM_FACT_TAG_MEASURES_STATS creation...;
                    %return;
                 %end;

       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the PAM_FACT_TAG_MEASURES_STATS table...";

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name="PAM_ASSET_LOC_MEASURE_FACT";
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the PAM_ASSET_LOC_MEASURE_FACT table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the PAM_ASSET_LOC_MEASURE_FACT table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            fedsql.execdirect result=res status=rc / 
                   query="create table &inp_caslib..PAM_FACT_TAG_MEASURES_STATS {options replace=true} as
                          select pam_tag_dim_rk, pam_date_dim_rk as tag_measure_dt, count(*) as record_cnt
                            from &inp_caslib..pam_asset_loc_measure_fact
                           group by pam_tag_dim_rk, pam_date_dim_rk";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating PAM_FACT_TAG_MEASURES_STATS."
                 ok_msg = " Step &step - NOTE: Table PAM_FACT_TAG_MEASURES_STATS created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;
%mend aiot_pam_fact_tag_measures_stat;

%macro aiot_pam_event_dim;
       /*=========================================================================================*/
       /* Create PAM_EVENT_DIM                                                                    */
       /*=========================================================================================*/
       %if (
            (&adj_event_table eq) and
            (&adj_event_measure_table eq)
           )
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Skipping PAM_EVENT_DIM creation...;
                    %return;
                 %end;

       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the PAM_EVENT_DIM table...";

            drop_list = '';
            if ("&adj_event_table" != '')
               then source_table = "&adj_event_table";
               else if ("&adj_event_measure_table" != '')
                       then do;
                              source_table = "&adj_event_measure_table";
                              drop_list = " &datetime_column ""&value_column""";
                            end;

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name=source_table;
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the "||upcase(source_table)||" table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the "||upcase(source_table)||" table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..PAM_EVENT_DIM (drop=(curr_tstamp"||drop_list||")) / overwrite=yes;
                                dcl integer      PAM_EVENT_DIM_RK;
                                dcl char(256)    EVENT_ID;
                                dcl varchar(256) EVENT_COLUMN_NM;
                                dcl char(&l_event_aggreg_missing_method) EVENT_DEF_AGGREG_METHOD_CD;
                                dcl integer      CURRENT_NUM;
                                dcl timestamp    PROCESS_DTTM having format datetime.;
                                dcl double       curr_tstamp;
                                retain curr_tstamp;

                                method init();
                                       PAM_EVENT_DIM_RK = 1;
                                       curr_tstamp = datetime();
                                end;
                                method run();
                                       set &inp_caslib.."""||source_table||""" (rename=(""&event_id_column"" as EVENT_ID));
                                        by EVENT_ID;
                                       if (first.EVENT_ID) 
                                          then do;
                                                 EVENT_COLUMN_NM = EVENT_ID;
                                                 EVENT_DEF_AGGREG_METHOD_CD = '&event_aggreg_missing_method';
                                                 CURRENT_NUM = 1;
                                                 PROCESS_DTTM = to_timestamp(curr_tstamp);
                                                 output;
                                                 PAM_EVENT_DIM_RK + 1;
                                               end;
                                end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating PAM_EVENT_DIM."
                 ok_msg = " Step &step - NOTE: Table PAM_EVENT_DIM created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;
%mend aiot_pam_event_dim;

%macro aiot_pam_asset_loc_event_fact;
       /*=========================================================================================*/
       /* Create PAM_ASSET_LOC_EVENT_FACT                                                         */
       /*=========================================================================================*/
       %if (
            (&adj_event_table eq) and
            (&adj_event_measure_table eq)
           )
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Skipping PAM_ASSET_LOC_EVENT_FACT creation...;
                    %return;
                 %end;

       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the PAM_ASSET_LOC_EVENT_FACT table...";

            if ("&adj_event_table" != '')
               then source_table = "&adj_event_table";
               else if ("&adj_event_measure_table" != '')
                       then source_table = "&adj_event_measure_table";

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name=source_table;
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the "||upcase(source_table)||" table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the "||upcase(source_table)||" table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name="PAM_ASSET_DIM";
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the PAM_ASSET_DIM table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the PAM_ASSET_DIM table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name="PAM_EVENT_DIM";
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the PAM_EVENT_DIM table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the PAM_EVENT_DIM table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            if (source_table = "&adj_event_table")
               then do;
                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..PAM_ASSET_LOC_EVENT_FACT (drop=(""&device_id_column"" ""&event_id_column"" curr_tstamp &datetime_column)) / overwrite=yes;
                                          dcl double       PAM_DATE_DIM_RK PAM_TIME_DIM_RK DATETIME;
                                          dcl double       PROCESS_DTTM having format datetime25.6;
                                          dcl double       EVENT_DTTM having format datetime25.6;
                                          dcl varchar(128) PAM_EVENT_ID;
                                          dcl double       curr_tstamp;
                                          retain curr_tstamp;

                                          method init();
                                                 curr_tstamp = datetime();
                                          end;
                                          method run();
                                                 set {select c.PAM_ASSET_DIM_RK, 
                                                             d.PAM_EVENT_DIM_RK,
                                                             a.""&event_id_column"" as PAM_EVENT_ID,
                                                             a.&datetime_column as EVENT_DTTM, 
                                                             a.""&value_column"" as EVENT_VALUE
                                                        from &inp_caslib..""&adj_event_measure_table"" a,
                                                             (select distinct ""&event_id_column""
                                                                from &inp_caslib..&adj_event_table) b,
                                                             &inp_caslib..PAM_ASSET_DIM c,
                                                             &inp_caslib..PAM_EVENT_DIM d
                                                       where a.""&event_id_column"" = b.""&event_id_column""
                                                         and a.""&device_id_column"" = c.PAM_ASSET_ID
                                                         and a.""&event_id_column"" = d.EVENT_ID
                                                         and b.""&event_id_column"" = d.EVENT_ID};
                                                 PAM_DATE_DIM_RK = datepart(EVENT_DTTM);
                                                 PAM_TIME_DIM_RK = timepart(EVENT_DTTM);
                                                 PROCESS_DTTM = curr_tstamp;
                                          end;
                                     enddata;";
                    end;
               else do;
                      /* PAM_ASSET_DIM_RK should be 0 */
                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..PAM_ASSET_LOC_EVENT_FACT (drop=(""&device_id_column"" ""&event_id_column"" curr_tstamp &datetime_column)) / overwrite=yes;
                                          dcl double    PAM_DATE_DIM_RK PAM_TIME_DIM_RK DATETIME;
                                          dcl double    PROCESS_DTTM having format datetime25.6;
                                          dcl double    EVENT_DTTM having format datetime25.6;
                                          dcl double    curr_tstamp;
                                          retain curr_tstamp;

                                          method init();
                                                 curr_tstamp = datetime();
                                          end;
                                          method run();
                                                 set {select b.PAM_ASSET_DIM_RK,
                                                             c.PAM_EVENT_DIM_RK,
                                                             a.""&event_id_column"",
                                                             a.&datetime_column as EVENT_DTTM, 
                                                             a.""&value_column"" as EVENT_VALUE
                                                        from &inp_caslib..""&adj_event_measure_table"" a,
                                                             &inp_caslib..PAM_ASSET_DIM b,
                                                             &inp_caslib..PAM_EVENT_DIM c
                                                       where a.""&event_id_column"" = c.EVENT_ID};
                                                 PAM_DATE_DIM_RK = datepart(EVENT_DTTM);
                                                 PAM_TIME_DIM_RK = timepart(EVENT_DTTM);
                                                 PROCESS_DTTM = curr_tstamp;
                                          end;
                                     enddata;";
                    end;
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating PAM_ASSET_LOC_EVENT_FACT."
                 ok_msg = " Step &step - NOTE: Table PAM_ASSET_LOC_EVENT_FACT created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;
%mend aiot_pam_asset_loc_event_fact;

%macro aiot_pam_fact_event_stats;
       /*=========================================================================================*/
       /* Create PAM_FACT_EVENT_STATS                                                             */
       /*=========================================================================================*/
       %if (
            (&adj_event_table eq) and
            (&adj_event_measure_table eq)
           )
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Skipping PAM_FACT_EVENT_STATS creation...;
                    %return;
                 %end;

       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the PAM_FACT_EVENT_STATS table...";

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name="PAM_ASSET_LOC_EVENT_FACT";
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the PAM_ASSET_LOC_EVENT_FACT table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the PAM_ASSET_LOC_EVENT_FACT table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            fedsql.execdirect result=res status=rc / 
                   query="create table &inp_caslib..PAM_FACT_EVENT_STATS {options replace=true} as
                          select pam_asset_dim_rk, pam_event_dim_rk, pam_date_dim_rk as event_fact_dt, count(*) as record_cnt
                            from &inp_caslib..PAM_ASSET_LOC_EVENT_FACT
                           group by 1, 2, 3;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating PAM_FACT_EVENT_STATS."
                 ok_msg = " Step &step - NOTE: Table PAM_FACT_EVENT_STATS created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;
%mend aiot_pam_fact_event_stats;

%macro aiot_process_hierarchies(hierarchy_table, table_type);
       /*=========================================================================================*/
       /* Process sensor hierarchies                                                              */
       /*=========================================================================================*/
       %if (&hierarchy_table eq)
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Skipping hierarchies for &table_type...;
                    %return;
                 %end;

       %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Processing hierarchies for &table_type...;
       /*=========================================================================================*/
       /* Let's make sure the hierarchy exists in memory.                                         */
       /*=========================================================================================*/
       proc cas;
            session "&_sessref_";
            retcode = 0;

            function check_hierarchy_structure(table_name);
                     check.node = FALSE;
                     check.parent = FALSE;
                     retcode.severity = 0;
                     retcode.reason = 0;
                     retcode.status = '';
                     retcode.statuscode = 0;

                     table.columninfo result=res / 
                           table=table_name;

                     colinfo = findtable(res);
     
                     do col over colinfo;
                        if (upcase(col.Column) == "&node_column")
                           then check.node = TRUE;
                           else if (upcase(col.Column) == "&parent_column")
                                then check.parent = TRUE;
                     end;

                     if (check.node and check.parent)
                        then retcode.severity = 0;
                        else retcode.severity = 2;

                     return(retcode);
            end;

            table.tableExists result=res status=rc /
                  caslib="&inp_caslib"
                  name="&hierarchy_table";
            rc.status = '';

            if (res.exists == 0)
               then rc.severity = 2;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while accessing the "||upcase("&hierarchy_table")||" table."
                 ok_msg = " Step &step - NOTE: Existence successfully verified for the "||upcase("&hierarchy_table")||" table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.ReturnCodeHandler result=res / 
                 ret_code = check_hierarchy_structure("&hierarchy_table")
                 err_msg = " Step &step - ERROR: The structure of the hierarchy table "||upcase("&hierarchy_table")||" must include the &node_column and &parent_column columns."
                 ok_msg = " Step &step - NOTE: Structure successfully verified for the "||upcase("&hierarchy_table")||" hierarchy table.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;

       /*=========================================================================================*/
       /* No hierarchy table or incorrect hierarchy structure? We are out...                      */
       /*=========================================================================================*/
       %if (&return_code > 0)
           %then %return;

       /*=========================================================================================*/
       /* Do this for double-byte character sets                                                  */
       /*=========================================================================================*/
       options validmemname=extend;

       libname &inp_caslib cas caslib=&inp_caslib;

       /*=========================================================================================*/
       /* Copy the hierarchy table to SAS. Calculating the number of rows is only needed in case  */
       /* hierarchy levels need to be calculated.                                                 */
       /*=========================================================================================*/
       data &hierarchy_table (index=(&node_column));
            set &inp_caslib..&hierarchy_table nobs=size;
            by &node_column &parent_column;
            call symput("hierarchy_count", size);
       run;

       /*=========================================================================================*/
       /* Do we have a level column?                                                              */
       /*=========================================================================================*/
       data _null_;
            dsid = open("&hierarchy_table");
            check = varnum(dsid,"&level_column");
            call symput("level_col_check", check);
       run;

       %if (&level_col_check)
           %then %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Column &level_column found in table %upcase(&hierarchy_table)....;
           %else %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Column &level_column not found in table %upcase(&hierarchy_table)....;

       %if (&level_col_check = 0)
           %then %do;
                    /*=========================================================================================*/
                    /* Calculate the hierarchy levels                                                          */
                    /*=========================================================================================*/
                    data work.&hierarchy_table (drop=&node_column &parent_column);

                         /*=========================================================================================*/
                         /* Read in SAS data set                                                                    */
                         /*=========================================================================================*/
                         set work.&hierarchy_table;

                         level_num = 1;
                         bkp_node = &node_column;
                         bkp_parent = &parent_column;

                         /*=========================================================================================*/
                         /* Process the table                                                                       */
                         /*=========================================================================================*/
                         do while (not missing(&parent_column));
                            &node_column = left(&parent_column);

                            /*=========================================================================================*/
                            /* Search the parent in the node list:                                                     */
                            /* - If not there, leave                                                                   */
                            /* - If found, increase the hierarchy level value for the node                             */
                            /*=========================================================================================*/
                            set work.&hierarchy_table key=&node_column;

                            if _IORC_ ne 0 
                               then leave;
                               else &level_column + 1;
                         end;

                         _error_ = 0;
                         rename bkp_node = &node_column;
                         rename bkp_parent = &parent_column;
                    run;
                 %end;

       /*=========================================================================================*/
       /* Process the now complete hierarchy table                                                */
       /*=========================================================================================*/
       proc sql noprint;
            select max(&level_column)-1, max(length(&node_column))
              into :max_level, :max_node_length
              from &hierarchy_table;
       quit;

       %if (%upcase(&table_type) = DEVICE)
           %then %do;
                    %let col_name_root = %sysfunc(kscan(&device_id_column, 1, '_'));
                    data &inp_caslib..lineage (keep=pam_asset_id &col_name_root:);
                 %end;
           %else %if (%upcase(&table_type) = EVENT)
                     %then %do;
                              %let col_name_root = %sysfunc(kscan(&event_id_column, 1, '_'));
                              data &inp_caslib..lineage (keep=event_id &col_name_root:);
                           %end;
                     %else %if (%upcase(&table_type) = SENSOR)
                           %then %do; 
                                    %let col_name_root = %sysfunc(kscan(&sensor_id_column, 1, '_'));
                                    data &inp_caslib..lineage (keep=tag_id &col_name_root:);
                                 %end;
            /*=========================================================================================*/
            /* Create an array to hold the maximum number of observations that will 'link' together.   */
            /*=========================================================================================*/
            array hierarchy[&max_level] $ &max_node_length;
            count = 1;

            /*=========================================================================================*/
            /* Read in SAS data set                                                                    */
            /*=========================================================================================*/
            set work.&hierarchy_table;

            /*=========================================================================================*/
            /* Save the node being processed                                                           */
            /*=========================================================================================*/
            %if (%upcase(&table_type) = DEVICE)
                 %then %do;
                          pam_asset_id = left(&node_column);
                       %end;
                 %else %if (%upcase(&table_type) = EVENT)
                           %then %do; 
                                    event_id = left(&node_column);
                                 %end;
                           %else %if (%upcase(&table_type) = SENSOR)
                                     %then %do; 
                                              tag_id = left(&node_column);
                                           %end;

            /*=========================================================================================*/
            /*  Process through the upper boundary of the array                                        */
            /*=========================================================================================*/
            do i = 1 to last while (count < dim(hierarchy));
               /*=========================================================================================*/
               /* Read the same data set again. Rename the variables to new names so they can be compared */
               /* to the variables coming in from the first SET statement. Direct access is performed via */
               /* the POINT= option.                                                                      */
               /*=========================================================================================*/
               set work.&hierarchy_table (rename=(&node_column=&node_column.1 &parent_column=&parent_column.1 &level_column=&level_column.1)) nobs=last point=i;

               /*=========================================================================================*/
               /* As we step through each observation, compare the value of PARENT to the value of NODE1  */
               /* (which is the new name for the NODE variable when the data set is read the next time).  */
               /* If they match, add a new entry to the array.                                            */
               /*=========================================================================================*/
               if (left(&parent_column) = left(&node_column.1)) 
                  then do;
                         count + 1;
                         hierarchy(&level_column.1) = left(&node_column.1);
                         &node_column = left(&node_column.1);
                         &parent_column = left(&parent_column.1);
                         i = 1;
                       end;
            end;

            count + 1;
            hierarchy(1) = left(parent);

            /*=========================================================================================*/
            /* Create a set of new variables to map the hierarchy, and populate them accordingly.      */
            /*=========================================================================================*/
            %let col_list =;
            %do i = 1 %to &max_level;
                if (hierarchy[&i] eq '')
                   then hierarchy[&i] = "&missing_char_val";
                %if (%upcase(&table_type) = DEVICE)
                    %then %do;
                             &col_name_root._&i._LVL = hierarchy&i.;
                             %let col_list = &col_list%str(,)&col_name_root._&i._LVL;
                          %end;
                    %else %if (%upcase(&table_type) = EVENT)
                              %then %do; 
                                       &col_name_root._&i._LVL = hierarchy&i.;
                                       %let col_list = &col_list%str(,)&col_name_root._&i._LVL;
                                    %end;
                              %else %if (%upcase(&table_type) = SENSOR)
                                        %then %do;
                                                 &col_name_root._&i._LVL = hierarchy&i.;
                                                 %let col_list = &col_list%str(,)&col_name_root._&i._LVL;
                                              %end;
            %end;
            %let i = 1;
       run;

       libname &inp_caslib clear;
       proc delete data=work.&hierarchy_table;
       run;

       /*=========================================================================================*/
       /* Re-create appropriate PAM_*_DIM.                                                        */
       /*=========================================================================================*/
       %if (%upcase(&table_type) = DEVICE)
           %then %do;
                    %let pam_dim_name = %str(PAM_ASSET_DIM);
                    %let pam_dim_name_temp = %str(PAM_ASSET_DIM_TMP);
                    %let rename_clause = %nrquote(rename=(%"&device_id_column%"=PAM_ASSET_ID));
                    %let by_clause = %str(PAM_ASSET_ID);
                    %let main_table = %str(&adj_device_table);
                 %end;
           %else %if (%upcase(&table_type) = EVENT)
                     %then %do; 
                              %let pam_dim_name = %str(PAM_EVENT_DIM);
                              %let pam_dim_name_temp = %str(PAM_EVENT_DIM_TMP);
                              %let rename_clause = %nrquote(rename=(%"&event_id_column%"=EVENT_ID));
                              %let by_clause = %str(EVENT_ID);
                              %let main_table = %str(&adj_event_table);
                           %end;
                     %else %if (%upcase(&table_type) = SENSOR)
                               %then %do; 
                                        %let pam_dim_name = %str(PAM_TAG_DIM);
                                        %let pam_dim_name_temp = %str(PAM_TAG_DIM_TMP);
                                        %let rename_clause = %nrquote(rename=(%"&sensor_id_column%"=TAG_ID));
                                        %let by_clause = %str(TAG_ID);
                                        %let main_table = %str(&adj_sensor_table);
                                     %end;

       proc cas;
            session "&_sessref_";

            /*=========================================================================================*/
            /* Rename PAM_*_DIM before replacing it.                                                   */
            /*=========================================================================================*/
            table.dropTable result=res / caslib="&inp_caslib" table="&pam_dim_name_temp" quiet=TRUE;

            table.alterTable result=res status=rc /
                  caslib="&inp_caslib"
                  name="&pam_dim_name"
                  rename="&pam_dim_name_temp";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while renaming &pam_dim_name."
                 ok_msg = " Step &step - NOTE: &pam_dim_name renamed successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..&pam_dim_name / overwrite=yes;
                                method run();
                                       merge &inp_caslib..&pam_dim_name_temp (in=a)
                                             &inp_caslib..""&main_table"" (in=b &rename_clause)
                                             &inp_caslib..lineage (in=c);
                                          by &by_clause;
                                       if (b and first.&by_clause);
                                end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while processing hierarchies."
                 ok_msg = " Step &step - NOTE: Hierarchies processed successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            %if (%upcase(&debug) = N)
                %then %do;
                         table.dropTable result=res / caslib="&inp_caslib" table="lineage" quiet=TRUE;
                      %end;

            /*=========================================================================================*/
            /* Update any *_LVL columns with the missing_char_val value in case any any records in the */
            /* main table don't have matches in the lineage table.                                     */
            /*=========================================================================================*/
            if ("&missing_char_val" ne "")
               then do;
                      count = 1;
                      updateList={};
                      whereClause="&col_name_root."||"_1_LVL = ''";
                      updateValue="'"||"&missing_char_val"||"'";
                         
                      table.columninfo result=res / 
                            table="&pam_dim_name";

                      colinfo = findtable(res);

                      do col over colinfo;
                         if (upcase(substr(reverse(col.Column),1,4)) = 'LVL_')
                            then do;
                                   updateVar.var=col.Column;
                                   updateVar.value=updateValue;
                                   updateList[count]=updateVar;
                                   count = count + 1;
                                 end;
                      end;

                      table.update result=res status=rc / 
                            table={
                            	   caslib="&inp_caslib" 
                                   name="&pam_dim_name"
                                   where=whereClause
                                  }
                            set=updateList;
                      rc.status = '';

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while updating &pam_dim_name."
                           ok_msg = " Step &step - NOTE: &pam_dim_name updated successfully.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;
                    end;

            run;
       quit;
%mend aiot_process_hierarchies;

%macro aiot_create_metadata;
       /*=========================================================================================*/
       /* Create FILTER_ATTRIBUTE                                                                 */
       /*        FILTER_ATTRIBUTE_GROUP                                                           */
       /*        FILTER_ATTRIBUTE_TREE                                                            */
       /*        LOCALIZATION_GROUP_DATASELECTION                                                 */
       /*        LOCALIZATION_VALUE_DATASELECTION                                                 */
       /*        LOCALIZATION_GROUP_MARTMETADATA                                                  */
       /*        LOCALIZATION_VALUE_MARTMETADATA                                                  */
       /*        TABLE_ATTRIBUTES                                                                 */
       /*        TABLE_META                                                                       */
       /*        TABLE_META_REFRESH_DATES                                                         */
       /*        TABLECOLUMN_META                                                                 */
       /*        TABLECOLUMN_ATTRIBUTES                                                           */
       /*=========================================================================================*/

       /*=========================================================================================*/
       /* Query for localized labels                                                              */
       /*=========================================================================================*/
       %let FILTER_VARIABLES_label = %sysfunc(sasmsg(&sasmsg_dataset, DSG000, NOQUOTE));
       %let FILTER_DEVICES_label = %sysfunc(sasmsg(&sasmsg_dataset, DEVICES_GRP, NOQUOTE));
       %let FILTER_SENSORS_label = %sysfunc(sasmsg(&sasmsg_dataset, SENSORS_GRP, NOQUOTE));
       %let FILTER_MEASURES_label = %sysfunc(sasmsg(&sasmsg_dataset, MEASURES_GRP, NOQUOTE));
       %let FILTER_EVENTS_label = %sysfunc(sasmsg(&sasmsg_dataset, EVENTS_GRP, NOQUOTE));
       %let FILTER_EVENT_MEASURES_label = %sysfunc(sasmsg(&sasmsg_dataset, EVENT_MEASURES_GRP, NOQUOTE));

       %let TABLE_DEVICES_label = %sysfunc(sasmsg(&sasmsg_dataset,ASSET,NOQUOTE));
       %let TABLE_MEASURES_label = %sysfunc(sasmsg(&sasmsg_dataset,TAGS,NOQUOTE));
       %let TABLE_EVENT_MEASURES_label = %sysfunc(sasmsg(&sasmsg_dataset,EVENTS,NOQUOTE));
       %let TABLE_TRANS_COLUMN_label = %sysfunc(sasmsg(&sasmsg_dataset,TAG_COLUMN_NM,NOQUOTE));
       %let TABLE_AGGREG_label = %sysfunc(sasmsg(&sasmsg_dataset,TAG_DEF_AGGREG_METHOD_CD,NOQUOTE));
       %let TABLE_INTER_label = %sysfunc(sasmsg(&sasmsg_dataset,TAG_DEF_INTERPOL_METHOD_CD,NOQUOTE));
       %let TABLE_MEASURE_DATE_label = %sysfunc(sasmsg(&sasmsg_dataset,MEASURE_DATE,NOQUOTE));
       %let TABLE_MEASURE_TIME_label = %sysfunc(sasmsg(&sasmsg_dataset,MEASURE_TIME,NOQUOTE));
       %let TABLE_EVENT_DATE_label = %sysfunc(sasmsg(&sasmsg_dataset,EVENT_DATE,NOQUOTE));
       %let TABLE_EVENT_TIME_label = %sysfunc(sasmsg(&sasmsg_dataset,EVENT_TIME,NOQUOTE));

       %let TABLE_SENSOR_ID_label = %sysfunc(tranwrd(%nrbquote(%sysfunc(ifc('&sensor_id_column' = '',SENSOR_ID,&sensor_id_column))),%str(_),&label_char_separator));
       %let TABLE_MEASURE_DTTM_label = %sysfunc(tranwrd(%nrbquote(%sysfunc(ifc('&datetime_column' = '',DATETIME,&datetime_column))),%str(_),&label_char_separator));
       %let TABLE_MEASURE_VALUE_label = %sysfunc(tranwrd(%nrbquote(%sysfunc(ifc('&value_column' = '',VALUE,&value_column))),%str(_),&label_char_separator));
       %let TABLE_DEVICE_ID_label = %sysfunc(tranwrd(%nrbquote(%sysfunc(ifc('&device_id_column' = '',DEVICE_ID,&device_id_column))),%str(_),&label_char_separator));
       %let TABLE_EVENT_ID_label = %sysfunc(tranwrd(%nrbquote(%sysfunc(ifc('&event_id_column' = '',EVENT_ID,&event_id_column))),%str(_),&label_char_separator));
       %let TABLE_EVENT_DTTM_label = %sysfunc(tranwrd(%nrbquote(%sysfunc(ifc('&datetime_column' = '',DATETIME,&datetime_column))),%str(_),&label_char_separator));
       %let TABLE_EVENT_VALUE_label = %sysfunc(tranwrd(%nrbquote(%sysfunc(ifc('&value_column' = '',VALUE,&value_column))),%str(_),&label_char_separator));

       proc cas; 
            session "&_sessref_";

            val_col_found = 0;
            dev_col_found = 0;
            extra_dev_cols = 0;
            extra_sen_cols = 0;
            extra_eve_cols = 0;
            extra_evm_cols = 0;
            extra_mea_cols = 0;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the metadata tables...";

            /*=========================================================================================*/
            /* Let's do some housekeeping...                                                           */
            /*                                                                                         */
            /* See if the value column is in the event measure file                                    */
            /*=========================================================================================*/
            if ("&adj_event_measure_table" != '')
               then do;
                      table.tableExists result=res status=rc /
                            caslib="&inp_caslib"
                            name="&adj_event_measure_table";
                      rc.status = '';

                      if (res.exists == 0)
                         then rc.severity = 2;

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while accessing the &adj_event_measure_table table."
                           ok_msg = " Step &step - NOTE: Existence successfully verified for the &adj_event_measure_table table.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;

                      table.columninfo result=event_measures / 
                            table="&adj_event_measure_table";

                      colinfo = findtable(event_measures);

                      do col over colinfo;
                         if (upcase(col.Column) = upcase("&value_column"))
                            then val_col_found = 1;
                            else do;
                                   if (
                                       (upcase(col.Column) ne upcase("&device_id_column")) and
                                       (upcase(col.Column) ne upcase("&event_id_column")) and
                                       (upcase(col.Column) ne upcase("&datetime_column"))
                                      )
                                      then do;
                                             extra_evm_cols = extra_evm_cols + 1;
                                             varname = 'extra_evm_col'||extra_evm_cols;
                                             call symputx(varname, col.Column);
                                           end;
                                 end;
                      end;
                    end;

            /*=========================================================================================*/
            /* Let's see if we have a device column.                                                   */
            /*                                                                                         */
            /* Do we have a device table?                                                              */
            /*=========================================================================================*/
            if ("&adj_device_table" != '')
               then do;
                      table.tableExists result=res status=rc /
                            caslib="&inp_caslib"
                            name="&adj_device_table";
                      rc.status = '';

                      if (res.exists == 0)
                         then rc.severity = 2;

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while accessing the &adj_device_table table."
                           ok_msg = " Step &step - NOTE: Existence successfully verified for the &adj_device_table table.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;

                      dev_col_found = 1;
                      /*=========================================================================================*/
                      /* Since we are here, collect the names of every column other than the device id           */
                      /*=========================================================================================*/
                      table.columninfo result=devices / 
                            table="&adj_device_table";

                      colinfo = findtable(devices);

                      do col over colinfo;
                         if (upcase(col.Column) ne upcase("&device_id_column"))
                            then do;
                                   extra_dev_cols = extra_dev_cols + 1;
                                   varname = 'extra_dev_col'||extra_dev_cols;
                                   call symputx(varname, col.Column);
                                 end;
                      end;
                    end;
               else do;
                      /*=========================================================================================*/
                      /* If not, is the device id in the measure table?                                          */
                      /*=========================================================================================*/
                      if ("&adj_measure_table" != '')
                         then do;
                                table.tableExists result=res status=rc /
                                      caslib="&inp_caslib"
                                      name="&adj_measure_table";
                                rc.status = '';

                                if (res.exists == 0)
                                   then rc.severity = 2;

                                AIot.ReturnCodeHandler result=res / 
                                     ret_code = rc
                                     err_msg = " Step &step - ERROR: An error occurred while accessing the &adj_measure_table table."
                                     ok_msg = " Step &step - NOTE: Existence successfully verified for the &adj_measure_table table.";

                                if (rc.severity > 1)
                                   then do;
                                          call symputx('return_code', 8);
                                          exit(0);
                                        end;

                                table.columninfo result=measures / 
                                      table="&adj_measure_table";
 
                                colinfo = findtable(measures);

                                do col over colinfo;
                                   if (upcase(col.Column) = upcase("&device_id_column"))
                                      then dev_col_found = 1;
                                end;
                              end;

                      /*=========================================================================================*/
                      /* Or is it in the event measure table?                                                    */
                      /*=========================================================================================*/
                      if (
                          ("&adj_event_measure_table" != '') and
                          (dev_col_found = 0)
                         )
                         then do;
                                colinfo = findtable(event_measures);
 
                                do col over colinfo;
                                   if (upcase(col.Column) = upcase("&device_id_column"))
                                      then dev_col_found = 1;
                                end;
                              end;

                      /*=========================================================================================*/
                      /* Or is it in the sensor table?                                                           */
                      /*=========================================================================================*/
                      if (
                          ("&adj_sensor_table" != '') and
                          (dev_col_found = 0)
                         )
                         then do;
                                table.tableExists result=res status=rc /
                                      caslib="&inp_caslib"
                                      name="&adj_sensor_table";
                                rc.status = '';

                                if (res.exists == 0)
                                   then rc.severity = 2;

                                AIot.ReturnCodeHandler result=res / 
                                     ret_code = rc
                                     err_msg = " Step &step - ERROR: An error occurred while accessing the &adj_sensor_table table."
                                     ok_msg = " Step &step - NOTE: Existence successfully verified for the &adj_sensor_table table.";

                                if (rc.severity > 1)
                                   then do;
                                          call symputx('return_code', 8);
                                          exit(0);
                                        end;

                                table.columninfo result=sensors / 
                                      table="&adj_sensor_table";

                                colinfo = findtable(sensors);

                                do col over colinfo;
                                   if (upcase(col.Column) = upcase("&device_id_column"))
                                      then dev_col_found = 2;
                                end;
                              end;

                      /*=========================================================================================*/
                      /* Or is it in the event table?                                                            */
                      /*=========================================================================================*/
                      if (
                          ("&adj_event_table" != '') and
                          (dev_col_found = 0)
                         )
                         then do;
                                table.tableExists result=res status=rc /
                                      caslib="&inp_caslib"
                                      name="&adj_event_table";
                                rc.status = '';

                                if (res.exists == 0)
                                   then rc.severity = 2;

                                AIot.ReturnCodeHandler result=res / 
                                     ret_code = rc
                                     err_msg = " Step &step - ERROR: An error occurred while accessing the &adj_event_table table."
                                     ok_msg = " Step &step - NOTE: Existence successfully verified for the &adj_event_table table.";

                                if (rc.severity > 1)
                                   then do;
                                          call symputx('return_code', 8);
                                          exit(0);
                                        end;

                                table.columninfo result=events / 
                                      table="&adj_event_table";

                                colinfo = findtable(events);

                                do col over colinfo;
                                   if (upcase(col.Column) = "&device_id_column")
                                      then dev_col_found = 3;
                                end;
                              end;
                    end;

            if ("&adj_device_hierarchy_table" != '')
               then do;
                      /*=========================================================================================*/
                      /* With device hierarchies, collect the name of every column in the hierarchy              */
                      /*=========================================================================================*/
                      table.tableExists result=res status=rc /
                            caslib="&inp_caslib"
                            name="PAM_ASSET_DIM";
                      rc.status = '';

                      if (res.exists == 0)
                         then rc.severity = 2;

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while accessing the PAM_ASSET_DIM table."
                           ok_msg = " Step &step - NOTE: Existence successfully verified for the PAM_ASSET_DIM table.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;

                      table.columninfo result=device_hierarchies / 
                            table="PAM_ASSET_DIM";

                      colinfo = findtable(device_hierarchies);

                      do col over colinfo;
                         if (upcase(substr(reverse(col.Column),1,4)) = 'LVL_')
                            then do;
                                   extra_dev_cols = extra_dev_cols + 1;
                                   varname = 'extra_dev_col'||extra_dev_cols;
                                   call symputx(varname, col.Column);
                                 end;
                      end;
                    end;

            if ("&adj_sensor_table" != '')
               then do;
                      /*=========================================================================================*/
                      /* Collect the names of every column other than the sensor and device ids                  */
                      /*=========================================================================================*/
                      table.tableExists result=res status=rc /
                            caslib="&inp_caslib"
                            name="&adj_sensor_table";
                      rc.status = '';

                      if (res.exists == 0)
                         then rc.severity = 2;

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while accessing the &adj_sensor_table table."
                           ok_msg = " Step &step - NOTE: Existence successfully verified for the &adj_sensor_table table.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;

                      table.columninfo result=sensors / 
                            table="&adj_sensor_table";

                      colinfo = findtable(sensors);

                      do col over colinfo;
                         if (
                             (upcase(col.Column) ne upcase("&sensor_id_column")) and
                             (upcase(col.Column) ne upcase("&device_id_column"))
                            )
                            then do;
                                   extra_sen_cols = extra_sen_cols + 1;
                                   varname = 'extra_sen_col'||extra_sen_cols;
                                   call symputx(varname, col.Column);
                                 end;
                      end;
                    end;

            if ("&adj_sensor_hierarchy_table" != '')
               then do;
                      /*=========================================================================================*/
                      /* With sensor hierarchies, collect the name of every column in the hierarchy              */
                      /*=========================================================================================*/
                      table.tableExists result=res status=rc /
                            caslib="&inp_caslib"
                            name="PAM_TAG_DIM";
                      rc.status = '';

                      if (res.exists == 0)
                         then rc.severity = 2;

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while accessing the PAM_TAG_DIM table."
                           ok_msg = " Step &step - NOTE: Existence successfully verified for the PAM_TAG_DIM table.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;

                      table.columninfo result=sensor_hierarchies / 
                            table="PAM_TAG_DIM";

                      colinfo = findtable(sensor_hierarchies);

                      do col over colinfo;
                         if (upcase(substr(reverse(col.Column),1,4)) = 'LVL_')
                            then do;
                                   extra_sen_cols = extra_sen_cols + 1;
                                   varname = 'extra_sen_col'||extra_sen_cols;
                                   call symputx(varname, col.Column);
                                 end;
                      end;
                    end;

            if ("&adj_event_table" != '')
               then do;
                      /*=========================================================================================*/
                      /* Collect the names of every column other than the sensor and device ids                  */
                      /*=========================================================================================*/
                      table.tableExists result=res status=rc /
                            caslib="&inp_caslib"
                            name="&adj_event_table";
                      rc.status = '';

                      if (res.exists == 0)
                         then rc.severity = 2;

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while accessing the &adj_event_table table."
                           ok_msg = " Step &step - NOTE: Existence successfully verified for the &adj_event_table table.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;

                      table.columninfo result=events / 
                            table="&adj_event_table";

                      colinfo = findtable(events);

                      do col over colinfo;
                         if (
                             (upcase(col.Column) ne upcase("&event_id_column")) and
                             (upcase(col.Column) ne upcase("&device_id_column"))
                            )
                            then do;
                                   extra_eve_cols = extra_eve_cols + 1;
                                   varname = 'extra_eve_col'||extra_eve_cols;
                                   call symputx(varname, col.Column);
                                 end;
                      end;
                    end;

            if ("&adj_event_hierarchy_table" != '')
               then do;
                      /*=========================================================================================*/
                      /* With event hierarchies, collect the name of every column in the hierarchy               */
                      /*=========================================================================================*/
                      table.tableExists result=res status=rc /
                            caslib="&inp_caslib"
                            name="PAM_EVENT_DIM";
                      rc.status = '';

                      if (res.exists == 0)
                         then rc.severity = 2;

                      AIot.ReturnCodeHandler result=res / 
                           ret_code = rc
                           err_msg = " Step &step - ERROR: An error occurred while accessing the PAM_EVENT_DIM table."
                           ok_msg = " Step &step - NOTE: Existence successfully verified for the PAM_EVENT_DIM table.";

                      if (rc.severity > 1)
                         then do;
                                call symputx('return_code', 8);
                                exit(0);
                              end;

                      table.columninfo result=event_hierarchies / 
                            table="PAM_EVENT_DIM";

                      colinfo = findtable(event_hierarchies);

                      do col over colinfo;
                         if (upcase(substr(reverse(col.Column),1,4)) = 'LVL_')
                            then do;
                                   extra_eve_cols = extra_eve_cols + 1;
                                   varname = 'extra_eve_col'||extra_eve_cols;
                                   call symputx(varname, col.Column);
                                 end;
                      end;
                    end;

            if ("&adj_measure_table" != '')
               then do;
                      /*=========================================================================================*/
                      /* Check for existance only if we haven't done it already                                  */
                      /*=========================================================================================*/
                      if ("&adj_device_table" != '')
                         then do;
                                table.tableExists result=res status=rc /
                                      caslib="&inp_caslib"
                                      name="&adj_measure_table";
                                rc.status = '';

                                if (res.exists == 0)
                                   then rc.severity = 2;

                                AIot.ReturnCodeHandler result=res / 
                                     ret_code = rc
                                     err_msg = " Step &step - ERROR: An error occurred while accessing the &adj_measure_table table."
                                     ok_msg = " Step &step - NOTE: Existence successfully verified for the &adj_measure_table table.";

                                if (rc.severity > 1)
                                   then do;
                                          call symputx('return_code', 8);
                                          exit(0);
                                        end;
                              end;

                      /*=========================================================================================*/
                      /* Collect the names of every column other than the sensor and device ids                  */
                      /*=========================================================================================*/
                      table.columninfo result=measures / 
                            table="&adj_measure_table";

                      colinfo = findtable(measures);

                      do col over colinfo;
                         if (
                             (upcase(col.Column) ne upcase("&value_column")) and
                             (upcase(col.Column) ne upcase("&sensor_id_column")) and
                             (upcase(col.Column) ne upcase("&device_id_column")) and
                             (upcase(col.Column) ne upcase("&datetime_column"))                             
                            )
                            then do;
                                   extra_mea_cols = extra_mea_cols + 1;
                                   varname = 'extra_mea_col'||extra_mea_cols;
                                   call symputx(varname, col.Column);
                                 end;
                      end;
                    end;

            call symputx('val_col_found', val_col_found);
            call symputx('dev_col_found', dev_col_found);
            call symputx('extra_dev_cols', extra_dev_cols);
            call symputx('extra_eve_cols', extra_eve_cols);
            call symputx('extra_evm_cols', extra_evm_cols);
            call symputx('extra_mea_cols', extra_mea_cols);
            call symputx('extra_sen_cols', extra_sen_cols);

            run;
       quit;

       libname casuser cas caslib=casuser;

       %if (
            (%eval(&extra_dev_cols) > 0) or
            (%eval(&extra_eve_cols) > 0) or
            (%eval(&extra_evm_cols) > 0) or
            (%eval(&extra_mea_cols) > 0) or
            (%eval(&extra_sen_cols) > 0)
           )        
           %then %do;
                    data casuser.filter_attr_extra_cols;
                         length FILTER_ATTR_ID            $144;
                         length CREATED_BY_NM             $400;
                         length CREATION_DTTM             8;
                         length MODIFIED_BY_NM            $400;
                         length MODIFIED_DTTM             8;
                         length COLUMN_NM                 $400;
                         length COMPONENT_CD              $400;
                         length COMPONENT_TYPE_CD         $400;
                         length DEFAULT_ORDER_NO          $160;
                         length INPUT_FIELD_LABEL_COL_NM  $400;
                         length INPUT_FIELD_TBL_NM        $400;
                         length INPUT_FIELD_TBL_WHERE_TXT $16000;
                         length INPUT_FIELD_TYPE_TXT      $160;
                         length INPUT_FIELD_VALUE_COL_NM  $400;
                         length LIMIT_FLG                 $4; 

                         %do i = 1 %to &extra_dev_cols;
                             FILTER_ATTR_ID = 'DEVATTR'||left(&i); COLUMN_NM = "&&extra_dev_col&i"; COMPONENT_CD = 'ASSET'; COMPONENT_TYPE_CD = 'ASSET'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = "&&extra_dev_col&i"; INPUT_FIELD_TBL_NM = 'PAM_ASSET_DIM'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = "&&extra_dev_col&i"; LIMIT_FLG = ''; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_eve_cols;
                             FILTER_ATTR_ID = 'EVTATTR'||left(&i); COLUMN_NM = "&&extra_eve_col&i"; COMPONENT_CD = 'EVENTS'; COMPONENT_TYPE_CD = 'EVENTS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = "&&extra_eve_col&i"; INPUT_FIELD_TBL_NM = 'PAM_EVENT_DIM'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = "&&extra_eve_col&i"; LIMIT_FLG = ''; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_evm_cols;
                             FILTER_ATTR_ID = 'EMATTR'||left(&i); COLUMN_NM = "&&extra_evm_col&i"; COMPONENT_CD = 'EVENTS'; COMPONENT_TYPE_CD = 'EVENTS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = "&&extra_evm_col&i"; INPUT_FIELD_TBL_NM = 'PAM_ASSET_LOC_EVENT_FACT'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = "&&extra_evm_col&i"; LIMIT_FLG = ''; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_mea_cols;
                             FILTER_ATTR_ID = 'SMATTR'||left(&i); COLUMN_NM = "&&extra_mea_col&i"; COMPONENT_CD = 'TAGS'; COMPONENT_TYPE_CD = 'TAGS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = "&&extra_mea_col&i"; INPUT_FIELD_TBL_NM = 'PAM_ASSET_LOC_MEASURE_FACT'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = "&&extra_mea_col&i"; LIMIT_FLG = ''; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_sen_cols;
                             FILTER_ATTR_ID = 'SENATTR'||left(&i); COLUMN_NM = "&&extra_sen_col&i"; COMPONENT_CD = 'TAGS'; COMPONENT_TYPE_CD = 'TAGS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = "&&extra_sen_col&i"; INPUT_FIELD_TBL_NM = 'PAM_TAG_DIM'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = "&&extra_sen_col&i"; LIMIT_FLG = ''; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                    run;

                    data casuser.table_colmeta_extra_cols;
                         length COLUMN_ID           $144;
                         length CREATED_BY_NM       $400;
                         length CREATION_DTTM       8;
                         length MODIFIED_BY_NM      $400;
                         length MODIFIED_DTTM       8;
                         length COLUMN_DATA_TYPE_CD 8;
                         length COLUMN_DESC         $4000;
                         length COLUMN_FMT_NM       $400;
                         length COLUMN_LABEL_TXT    $1000;
                         length COLUMN_NM           $400;
                         length TABLE_ID            $144;
                         length SOLUTION_CD         $400;

                         %do i = 1 %to &extra_dev_cols;
                             COLUMN_ID = 'DEVATTR'||left(&i); COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = "&&extra_dev_col&i"; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = "&&extra_dev_col&i"; COLUMN_NM = "&&extra_dev_col&i"; TABLE_ID = 'ASSET'; SOLUTION_CD = 'APA'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_eve_cols;
                             COLUMN_ID = 'EVTATTR'||left(&i); COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = "&&extra_eve_col&i"; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = "&&extra_eve_col&i"; COLUMN_NM = "&&extra_eve_col&i"; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_evm_cols;
                             COLUMN_ID = 'EMATTR'||left(&i); COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = "&&extra_evm_col&i"; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = "&&extra_evm_col&i"; COLUMN_NM = "&&extra_evm_col&i"; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_mea_cols;
                             COLUMN_ID = 'SMATTR'||left(&i); COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = "&&extra_mea_col&i"; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = "&&extra_mea_col&i"; COLUMN_NM = "&&extra_mea_col&i"; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_sen_cols;
                             COLUMN_ID = 'SENATTR'||left(&i); COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = "&&extra_sen_col&i"; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = "&&extra_sen_col&i"; COLUMN_NM = "&&extra_sen_col&i"; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;

                    run;

                    data casuser.table_colattr_extra_cols;
                         length TABLECOLUMN_ATTR_ID    $144;
                         length CREATED_BY_NM          $400;
                         length CREATION_DTTM          8;
                         length MODIFIED_BY_NM         $400;
                         length MODIFIED_DTTM          8;
                         length ATTRIBUTE_DATA_TYPE_CD 8;
                         length ATTRIBUTE_NM           $400;
                         length COLUMN_ID              $144;
                         length ATTRIBUTE_VAL          $2000;

                         %do i = 1 %to &extra_dev_cols;
                             TABLECOLUMN_ATTR_ID = trim('DEVATTR'||trim(&i))||'_FACT_TABLE'; COLUMN_ID = 'DEVATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('DEVATTR'||trim(&i))||'_FACT_COLUMN'; COLUMN_ID = 'DEVATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM_RK'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('DEVATTR'||trim(&i))||'_DIM_TABLE'; COLUMN_ID = 'DEVATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('DEVATTR'||trim(&i))||'_DIM_COLUMN'; COLUMN_ID = 'DEVATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = "&&extra_dev_col&i"; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('DEVATTR'||trim(&i))||'_DIM_PRIMARYKEY'; COLUMN_ID = 'DEVATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM_RK'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_eve_cols;
                             TABLECOLUMN_ATTR_ID = trim('EVTATTR'||trim(&i))||'_FACT_TABLE'; COLUMN_ID = 'EVTATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('EVTATTR'||trim(&i))||'_FACT_COLUMN'; COLUMN_ID = 'EVTATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('EVTATTR'||trim(&i))||'_DIM_TABLE'; COLUMN_ID = 'EVTATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('EVTATTR'||trim(&i))||'_DIM_COLUMN'; COLUMN_ID = 'EVTATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = "&&extra_eve_col&i"; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('EVTATTR'||trim(&i))||'_DIM_PRIMARYKEY'; COLUMN_ID = 'EVTATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_evm_cols;
                             TABLECOLUMN_ATTR_ID = trim('EMATTR'||trim(&i))||'_FACT_TABLE'; COLUMN_ID = 'EMATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('EMATTR'||trim(&i))||'_FACT_COLUMN'; COLUMN_ID = 'EMATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = "&&extra_evm_col&i"; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_mea_cols;
                             TABLECOLUMN_ATTR_ID = trim('SMATTR'||trim(&i))||'_FACT_TABLE'; COLUMN_ID = 'SMATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('SMATTR'||trim(&i))||'_FACT_COLUMN'; COLUMN_ID = 'SMATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = "&&extra_mea_col&i"; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                         %do i = 1 %to &extra_sen_cols;
                             TABLECOLUMN_ATTR_ID = trim('SENATTR'||left(&i))||'_FACT_TABLE'; COLUMN_ID = 'SENATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('SENATTR'||left(&i))||'_FACT_COLUMN'; COLUMN_ID = 'SENATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('SENATTR'||left(&i))||'_DIM_TABLE'; COLUMN_ID = 'SENATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_TAG_DIM'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('SENATTR'||left(&i))||'_DIM_COLUMN'; COLUMN_ID = 'SENATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = "&&extra_sen_col&i"; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                             TABLECOLUMN_ATTR_ID = trim('SENATTR'||left(&i))||'_DIM_PRIMARYKEY'; COLUMN_ID = 'SENATTR'||left(&i); ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = "&sysuserid"; CREATION_DTTM = datetime(); MODIFIED_BY_NM = "&sysuserid"; MODIFIED_DTTM = datetime(); output;
                         %end;
                    run;
                 %end;
           %else %do;
                    data casuser.filter_attr_extra_cols;
                         length FILTER_ATTR_ID            $144;
                         length CREATED_BY_NM             $400;
                         length CREATION_DTTM             8;
                         length MODIFIED_BY_NM            $400;
                         length MODIFIED_DTTM             8;
                         length COLUMN_NM                 $400;
                         length COMPONENT_CD              $400;
                         length COMPONENT_TYPE_CD         $400;
                         length DEFAULT_ORDER_NO          $160;
                         length INPUT_FIELD_LABEL_COL_NM  $400;
                         length INPUT_FIELD_TBL_NM        $400;
                         length INPUT_FIELD_TBL_WHERE_TXT $16000;
                         length INPUT_FIELD_TYPE_TXT      $160;
                         length INPUT_FIELD_VALUE_COL_NM  $400;
                         length LIMIT_FLG                 $4; 
                    run;

                    data casuser.table_colmeta_extra_cols;
                         length COLUMN_ID           $144;
                         length CREATED_BY_NM       $400;
                         length CREATION_DTTM       8;
                         length MODIFIED_BY_NM      $400;
                         length MODIFIED_DTTM       8;
                         length COLUMN_DATA_TYPE_CD 8;
                         length COLUMN_DESC         $4000;
                         length COLUMN_FMT_NM       $400;
                         length COLUMN_LABEL_TXT    $1000;
                         length COLUMN_NM           $400;
                         length TABLE_ID            $144;
                         length SOLUTION_CD         $400;
                    run;

                    data casuser.table_colattr_extra_cols;
                         length TABLECOLUMN_ATTR_ID    $144;
                         length CREATED_BY_NM          $400;
                         length CREATION_DTTM          8;
                         length MODIFIED_BY_NM         $400;
                         length MODIFIED_DTTM          8;
                         length ATTRIBUTE_DATA_TYPE_CD 8;
                         length ATTRIBUTE_NM           $400;
                         length COLUMN_ID              $144;
                         length ATTRIBUTE_VAL          $2000;
                    run;
                 %end;

       /*=========================================================================================*/
       /* Load the message table that will be used to create localization info                    */
       /*=========================================================================================*/
       data &cas_sasmsg_dataset;
            set &sasmsg_dataset;
       run;

       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the FILTER_ATTRIBUTE table...";

            datastep.runcode result=res status=rc / 
                     code = "data &inp_caslib..FILTER_ATTRIBUTE;
                                  length FILTER_ATTR_ID            $144   ;
                                  length CREATED_BY_NM             $400   ;
                                  format CREATION_DTTM             datetime25.6;
                                  length MODIFIED_BY_NM            $400   ;
                                  format MODIFIED_DTTM             datetime25.6;
                                  length COLUMN_NM                 $400   ;
                                  length COMPONENT_CD              $400   ;
                                  length COMPONENT_TYPE_CD         $400   ;
                                  length DEFAULT_ORDER_NO          $160   ;
                                  length INPUT_FIELD_LABEL_COL_NM  $400   ;
                                  length INPUT_FIELD_TBL_NM        $400   ;
                                  length INPUT_FIELD_TBL_WHERE_TXT $16000 ;
                                  length INPUT_FIELD_TYPE_TXT      $160   ;
                                  length INPUT_FIELD_VALUE_COL_NM  $400   ;
                                  length LIMIT_FLG                 $4     ;

                                  if (_n_ = 1)
                                     then do;
                                            FILTER_ATTR_ID = 'ASSET'; COLUMN_NM = 'PAM_ASSET_ID'; COMPONENT_CD = 'ASSET'; COMPONENT_TYPE_CD = 'ASSET'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = 'PAM_ASSET_ID'; INPUT_FIELD_TBL_NM = 'PAM_ASSET_DIM'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = 'PAM_ASSET_ID'; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;

                                            if ('&adj_measure_table' ne '')
                                               then do;
                                                      FILTER_ATTR_ID = 'VALUE'; COLUMN_NM = 'LOC_MEASURE_VALUE'; COMPONENT_CD = 'TAGS'; COMPONENT_TYPE_CD = 'TAGS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = ''; INPUT_FIELD_TBL_NM = ''; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'decimal'; INPUT_FIELD_VALUE_COL_NM = ''; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'DTTM'; COLUMN_NM = 'MEASURE_DTTM'; COMPONENT_CD = 'TAGS'; COMPONENT_TYPE_CD = 'TAGS'; DEFAULT_ORDER_NO = 'display_order'; INPUT_FIELD_LABEL_COL_NM = 'DATE_PERIOD_LABEL'; INPUT_FIELD_TBL_NM = 'Predefined_date'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'datetime'; INPUT_FIELD_VALUE_COL_NM = 'DATE_PERIOD_MACRO_CALL_TEXT'; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'DATE'; COLUMN_NM = 'MEASURE_DATE'; COMPONENT_CD = 'TAGS'; COMPONENT_TYPE_CD = 'TAGS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = ''; INPUT_FIELD_TBL_NM = ''; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'date'; INPUT_FIELD_VALUE_COL_NM = ''; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'TIME'; COLUMN_NM = 'MEASURE_TIME'; COMPONENT_CD = 'TAGS'; COMPONENT_TYPE_CD = 'TAGS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = ''; INPUT_FIELD_TBL_NM = ''; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'time'; INPUT_FIELD_VALUE_COL_NM = ''; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'TAG'; COLUMN_NM = 'TAG_ID'; COMPONENT_CD = 'TAGS'; COMPONENT_TYPE_CD = 'TAGS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = 'TAG_ID'; INPUT_FIELD_TBL_NM = 'PAM_TAG_DIM'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = 'TAG_ID'; LIMIT_FLG = '1'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'TASSET'; COLUMN_NM = 'PAM_ASSET_ID'; COMPONENT_CD = 'TAGS'; COMPONENT_TYPE_CD = 'TAGS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = 'PAM_ASSET_ID'; INPUT_FIELD_TBL_NM = 'PAM_TAG_DIM'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = 'PAM_ASSET_ID'; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                    end;
                                            if ('&adj_event_measure_table' ne '')
                                               then do;
                                                      FILTER_ATTR_ID = 'EVENT_VALUE'; COLUMN_NM = 'EVENT_VALUE'; COMPONENT_CD = 'EVENTS'; COMPONENT_TYPE_CD = 'EVENTS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = ''; INPUT_FIELD_TBL_NM = ''; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'decimal'; INPUT_FIELD_VALUE_COL_NM = ''; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'EVENT_DTTM'; COLUMN_NM = 'EVENT_DTTM'; COMPONENT_CD = 'EVENTS'; COMPONENT_TYPE_CD = 'EVENTS'; DEFAULT_ORDER_NO = 'display_order'; INPUT_FIELD_LABEL_COL_NM = 'DATE_PERIOD_LABEL'; INPUT_FIELD_TBL_NM = 'Predefined_date'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'datetime'; INPUT_FIELD_VALUE_COL_NM = 'DATE_PERIOD_MACRO_CALL_TEXT'; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'EVENT_DATE'; COLUMN_NM = 'EVENT_DATE'; COMPONENT_CD = 'EVENTS'; COMPONENT_TYPE_CD = 'EVENTS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = ''; INPUT_FIELD_TBL_NM = ''; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'date'; INPUT_FIELD_VALUE_COL_NM = ''; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'EVENT_TIME'; COLUMN_NM = 'EVENT_TIME'; COMPONENT_CD = 'EVENTS'; COMPONENT_TYPE_CD = 'EVENTS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = ''; INPUT_FIELD_TBL_NM = ''; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'time'; INPUT_FIELD_VALUE_COL_NM = ''; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'EVENT'; COLUMN_NM = 'EVENT_ID'; COMPONENT_CD = 'EVENTS'; COMPONENT_TYPE_CD = 'EVENTS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = 'EVENT_ID'; INPUT_FIELD_TBL_NM = 'PAM_EVENT_DIM'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = 'EVENT_ID'; LIMIT_FLG = '1'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      FILTER_ATTR_ID = 'EASSET'; COLUMN_NM = 'PAM_ASSET_ID'; COMPONENT_CD = 'EVENTS'; COMPONENT_TYPE_CD = 'EVENTS'; DEFAULT_ORDER_NO = ''; INPUT_FIELD_LABEL_COL_NM = 'PAM_ASSET_ID'; INPUT_FIELD_TBL_NM = 'PAM_EVENT_DIM'; INPUT_FIELD_TBL_WHERE_TXT = ''; INPUT_FIELD_TYPE_TXT = 'dialoglist'; INPUT_FIELD_VALUE_COL_NM = 'PAM_ASSET_ID'; LIMIT_FLG = ''; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                    end;
                                            if (
                                                (&extra_dev_cols > 0) or
                                                (&extra_eve_cols > 0) or
                                                (&extra_evm_cols > 0) or
                                                (&extra_mea_cols > 0) or
                                                (&extra_sen_cols > 0)
                                               )        
                                               then do until (LastObs);
                                                       set casuser.filter_attr_extra_cols end=lastobs;
                                                       output;
                                                    end;
                                          end;
                             run;"
                     single="YES";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating FILTER_ATTRIBUTE."
                 ok_msg = " Step &step - NOTE: Table FILTER_ATTRIBUTE created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the FILTER_ATTRIBUTE_GROUP table...";

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..FILTER_ATTRIBUTE_GROUP / overwrite=yes;
                                dcl char(144)  FILTER_ATTR_GRP_ID;
                                dcl char(400)  CREATED_BY_NM;
                                dcl double     CREATION_DTTM having format datetime25.6;
                                dcl char(400)  MODIFIED_BY_NM;
                                dcl double     MODIFIED_DTTM having format datetime25.6;
                                dcl char(12)   FILTER_ATTR_GRP_CHILD_TYPE_CD;
                                dcl char(4)    MUT_EXC_GRP_FLG;
                                dcl char(1000) FILTER_ATTR_GRP_LABEL_TXT;

                           method run();
                                  FILTER_ATTR_GRP_ID = 'DSG000'; FILTER_ATTR_GRP_CHILD_TYPE_CD = 'G'; MUT_EXC_GRP_FLG = '0'; FILTER_ATTR_GRP_LABEL_TXT = '&FILTER_VARIABLES_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                  FILTER_ATTR_GRP_ID = 'DEVICES_GRP'; FILTER_ATTR_GRP_CHILD_TYPE_CD = 'RA'; MUT_EXC_GRP_FLG = '0'; FILTER_ATTR_GRP_LABEL_TXT = '&FILTER_DEVICES_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                  FILTER_ATTR_GRP_ID = 'SENSORS_GRP'; FILTER_ATTR_GRP_CHILD_TYPE_CD = 'RA'; MUT_EXC_GRP_FLG = '0'; FILTER_ATTR_GRP_LABEL_TXT = '&FILTER_SENSORS_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                  FILTER_ATTR_GRP_ID = 'MEASURES_GRP'; FILTER_ATTR_GRP_CHILD_TYPE_CD = 'A'; MUT_EXC_GRP_FLG = '0'; FILTER_ATTR_GRP_LABEL_TXT = '&FILTER_MEASURES_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                  FILTER_ATTR_GRP_ID = 'EVENTS_GRP'; FILTER_ATTR_GRP_CHILD_TYPE_CD = 'RA'; MUT_EXC_GRP_FLG = '0'; FILTER_ATTR_GRP_LABEL_TXT = '&FILTER_EVENTS_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                  FILTER_ATTR_GRP_ID = 'EVENT_MEASURES_GRP'; FILTER_ATTR_GRP_CHILD_TYPE_CD = 'A'; MUT_EXC_GRP_FLG = '0'; FILTER_ATTR_GRP_LABEL_TXT = '&FILTER_EVENT_MEASURES_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                           end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating FILTER_ATTRIBUTE_GROUP."
                 ok_msg = " Step &step - NOTE: Table FILTER_ATTRIBUTE_GROUP created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the FILTER_ATTRIBUTE_TREE table...";

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..FILTER_ATTRIBUTE_TREE (drop=(i)) / overwrite=yes;
                                dcl char(400)  CHILD_ID;
                                dcl char(400)  COMPONENT_TYPE_CD;
                                dcl char(400)  PARENT_ID;
                                dcl char(400)  CREATED_BY_NM;
                                dcl double     CREATION_DTTM having format datetime25.6;
                                dcl char(400)  MODIFIED_BY_NM;
                                dcl double     MODIFIED_DTTM having format datetime25.6;
                                dcl double     CHILD_LEVEL_NO having format 11.;
                                dcl char(12)   CHILD_TYPE_CD;
                                dcl char(4000) COMPONENT_CD;
                                dcl char(4000) DEFAULT_VALUE_TXT;
                                dcl char(4)    INCLUDE_EXCLUDE_FLG;
                                dcl double     PEER_LEVEL_NO having format 11.;
                                dcl double     I;

                           method run();
                                  if ('&adj_measure_table' ne '')
                                     then do;
                                            CHILD_ID = 'SENSORS_GRP'; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'DSG000'; CHILD_LEVEL_NO = 1; CHILD_TYPE_CD = 'G'; COMPONENT_CD = ''; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 2; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'MEASURES_GRP'; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'DSG000'; CHILD_LEVEL_NO = 1; CHILD_TYPE_CD = 'G'; COMPONENT_CD = ''; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 3; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'TAG'; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'SENSORS_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'RA'; COMPONENT_CD = 'TAGS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 100; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'DTTM'; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'MEASURES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'A'; COMPONENT_CD = 'TAGS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 1; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'TIME'; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'MEASURES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'A'; COMPONENT_CD = 'TAGS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 2; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'VALUE'; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'MEASURES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'A'; COMPONENT_CD = 'TAGS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 3; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                          end;
                                  if ('&adj_event_measure_table' ne '')
                                     then do;
                                            CHILD_ID = 'EVENTS_GRP'; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'DSG000'; CHILD_LEVEL_NO = 1; CHILD_TYPE_CD = 'G'; COMPONENT_CD = ''; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 4; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'EVENT_MEASURES_GRP'; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'DSG000'; CHILD_LEVEL_NO = 1; CHILD_TYPE_CD = 'G'; COMPONENT_CD = ''; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 5; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'EVENT'; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'EVENTS_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'RA'; COMPONENT_CD = ''; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 100; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'EVENT_DTTM'; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'EVENT_MEASURES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'A'; COMPONENT_CD = ''; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 1; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'EVENT_TIME'; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'EVENT_MEASURES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'A'; COMPONENT_CD = ''; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 2; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                          end;
                                  if (&val_col_found eq 1)
                                     then do;
                                            CHILD_ID = 'EVENT_VALUE'; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'EVENT_MEASURES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'A'; COMPONENT_CD = 'EVENTS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 3; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                          end;
                                  if (&dev_col_found eq 1)
                                     then do;
                                            CHILD_ID = 'DEVICES_GRP'; COMPONENT_TYPE_CD = 'ASSET'; PARENT_ID = 'DSG000'; CHILD_LEVEL_NO = 1; CHILD_TYPE_CD = 'G'; COMPONENT_CD = ''; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 1; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            CHILD_ID = 'ASSET'; COMPONENT_TYPE_CD = 'ASSET'; PARENT_ID = 'DEVICES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'RA'; COMPONENT_CD = 'ASSET'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 100; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                          end;      
                                  if (&dev_col_found eq 2)
                                     then do;
                                            CHILD_ID = 'TASSET'; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'SENSORS_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'RA'; COMPONENT_CD = 'TAGS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 99; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;                           
                                          end;
                                  if (&dev_col_found eq 3)
                                     then do;
                                            CHILD_ID = 'EASSET'; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'EVENTS_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'RA'; COMPONENT_CD = 'EVENTS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 99; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;                           
                                          end;
                                  if (&extra_dev_cols > 0)
                                     then do;
                                            do i = 1 to &extra_dev_cols;
                                               CHILD_ID = 'DEVATTR'||i; COMPONENT_TYPE_CD = 'ASSET'; PARENT_ID = 'DEVICES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'RA'; COMPONENT_CD = 'ASSET'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = i; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            end;                                               
                                          end;
                                  if (&extra_eve_cols > 0)
                                     then do;
                                            do i = 1 to &extra_eve_cols;
                                               CHILD_ID = 'EVTATTR'||i; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'EVENTS_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'RA'; COMPONENT_CD = 'EVENTS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = i; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            end;
                                          end;
                                  if (&extra_sen_cols > 0)
                                     then do;
                                            do i = 1 to &extra_sen_cols;
                                               CHILD_ID = 'SENATTR'||i; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'SENSORS_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'RA'; COMPONENT_CD = 'TAGS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = i; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            end;
                                          end;
                                  if (&extra_mea_cols > 0)
                                     then do;
                                            do i = 1 to &extra_mea_cols;
                                               CHILD_ID = 'SMATTR'||i; COMPONENT_TYPE_CD = 'TAGS'; PARENT_ID = 'MEASURES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'A'; COMPONENT_CD = 'TAGS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 5; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            end;
                                          end;
                                  if (&extra_evm_cols > 0)
                                     then do;
                                            do i = 1 to &extra_evm_cols;
                                               CHILD_ID = 'EMATTR'||i; COMPONENT_TYPE_CD = 'EVENTS'; PARENT_ID = 'EVENT_MEASURES_GRP'; CHILD_LEVEL_NO = 2; CHILD_TYPE_CD = 'A'; COMPONENT_CD = 'EVENTS'; DEFAULT_VALUE_TXT = ''; INCLUDE_EXCLUDE_FLG = ''; PEER_LEVEL_NO = 5; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            end;
                                          end;
                           end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating FILTER_ATTRIBUTE_TREE."
                 ok_msg = " Step &step - NOTE: Table FILTER_ATTRIBUTE_TREE created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the LOCALIZATION_GROUP_DATASELECTION table...";

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..LOCALIZATION_GROUP_DATASELECTION / overwrite=yes;
                                dcl char(1020) LOCALIZATION_GROUP_ID; 
                                dcl char(2000) DESCRIPTION_TXT;
                                dcl char(400)  NAME_NM;

                           method run();
                                  LOCALIZATION_GROUP_ID = 'G1'; DESCRIPTION_TXT = 'Filter Attribute Group'; NAME_NM = 'filter_attribute_group'; output;
                                  LOCALIZATION_GROUP_ID = 'G4'; DESCRIPTION_TXT = 'Pre-Defined Date'; NAME_NM = 'predefined_date'; output;
                           end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating LOCALIZATION_GROUP_DATASELECTION."
                 ok_msg = " Step &step - NOTE: Table LOCALIZATION_GROUP_DATASELECTION created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the LOCALIZATION_VALUE_DATASELECTION table...";

            datastep.runcode result=res status=rc / 
                     code = "data &inp_caslib..LOCALIZATION_VALUE_DATASELECTION (keep=LOCALIZATION_VALUE_ID BASENAME_NM LOCALE_NM VALUE_TXT LOCALIZATION_GROUP_ID);
                                  length LOCALIZATION_VALUE_ID $144;
                                  length BASENAME_NM           $1000; 
                                  length LOCALE_NM             $400;
                                  length VALUE_TXT             $32767;
                                  length LOCALIZATION_GROUP_ID $1020; 

                                  set &cas_sasmsg_dataset (where=(key not in ('TAG_COLUMN_NM',
                                                                              'TAG_DEF_AGGREG_METHOD_CD',
                                                                              'TAG_DEF_INTERPOL_METHOD_CD',
                                                                              'MEASURE_DATE',
                                                                              'MEASURE_TIME',
                                                                              'EVENT_DATE',
                                                                              'EVENT_TIME',
                                                                              'ASSET',
                                                                              'TAGS',
                                                                              'EVENTS')));

                                      LOCALIZATION_VALUE_ID = cats('V',_n_);
                                      LOCALE_NM = locale;
                                      VALUE_TXT = text;

                                      if key in ('DSG000','DEVICES_GRP','SENSORS_GRP','MEASURES_GRP','EVENTS_GRP','EVENT_MEASURES_GRP') 
                                         then do;
                                                BASENAME_NM = key;
                                                LOCALIZATION_GROUP_ID = 'G1';
                                              end;
                                         else do;
                                                BASENAME_NM = cats('%nrstr(%wrna_calcperiods(p_dateVarName=g_dwLstRfrshDt,p_period=',key,'))');
                                                LOCALIZATION_GROUP_ID = 'G4';
                                              end;
                             run;"
                     single="YES";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating LOCALIZATION_VALUE_DATASELECTION."
                 ok_msg = " Step &step - NOTE: Table LOCALIZATION_VALUE_DATASELECTION created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the LOCALIZATION_GROUP_MARTMETADATA table...";

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..LOCALIZATION_GROUP_MARTMETADATA / overwrite=yes;
                                dcl char(1020) LOCALIZATION_GROUP_ID; 
                                dcl char(2000) DESCRIPTION_TXT;
                                dcl char(400)  NAME_NM;

                           method run();
                                  LOCALIZATION_GROUP_ID = 'G2'; DESCRIPTION_TXT = 'Table Column Meta'; NAME_NM = 'table_column_meta'; output;
                                  LOCALIZATION_GROUP_ID = 'G3'; DESCRIPTION_TXT = 'Table Meta'; NAME_NM = 'table_meta'; output;
                           end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating LOCALIZATION_GROUP_MARTMETADATA."
                 ok_msg = " Step &step - NOTE: Table LOCALIZATION_GROUP_MARTMETADATA created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the LOCALIZATION_VALUE_MARTMETADATA table...";

            datastep.runcode result=res status=rc / 
                     code = "data &inp_caslib..LOCALIZATION_VALUE_MARTMETADATA (keep=LOCALIZATION_VALUE_ID BASENAME_NM LOCALE_NM VALUE_TXT LOCALIZATION_GROUP_ID);
                                  length LOCALIZATION_VALUE_ID $144;
                                  length BASENAME_NM           $1000; 
                                  length LOCALE_NM             $400;
                                  length VALUE_TXT             $32767;
                                  length LOCALIZATION_GROUP_ID $1020; 

                                  set &cas_sasmsg_dataset (where=(key in ('TAG_COLUMN_NM',
                                                                          'TAG_DEF_AGGREG_METHOD_CD',
                                                                          'TAG_DEF_INTERPOL_METHOD_CD',
                                                                          'MEASURE_DATE',
                                                                          'MEASURE_TIME',
                                                                          'EVENT_DATE',
                                                                          'EVENT_TIME',
                                                                          'ASSET',
                                                                          'TAGS',
                                                                          'EVENTS')));

                                      LOCALIZATION_VALUE_ID = cats('V',_n_+10000);
                                      BASENAME_NM = key;
                                      LOCALE_NM = locale;
                                      VALUE_TXT = text;

                                      if key in ('ASSET','TAGS','EVENTS')
                                         then LOCALIZATION_GROUP_ID = 'G3';   
                                         else LOCALIZATION_GROUP_ID = 'G2';
                             run;"
                     single="YES";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating LOCALIZATION_VALUE_MARTMETADATA."
                 ok_msg = " Step &step - NOTE: Table LOCALIZATION_VALUE_MARTMETADATA created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the TABLE_ATTRIBUTES table...";

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..TABLE_ATTRIBUTES / overwrite=yes;
                                dcl char(144)  TABLE_ATTR_ID;
                                dcl char(400)  CREATED_BY_NM;
                                dcl double     CREATION_DTTM having format datetime25.6;
                                dcl char(400)  MODIFIED_BY_NM;
                                dcl double     MODIFIED_DTTM having format datetime25.6;
                                dcl double     ATTRIBUTE_DATA_TYPE_CD having format 11.;
                                dcl char(400)  ATTRIBUTE_NM;
                                dcl char(144)  TABLE_ID;
                                dcl char(2000) ATTRIBUTE_VAL;

                           method run();
                                  TABLE_ATTR_ID = 'ASSET_COMPONENT_GROUP'; TABLE_ID = 'ASSET'; ATTRIBUTE_NM = 'COMPONENT_GROUP'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'ASSET'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                  TABLE_ATTR_ID = 'ASSET_COMPONENT_TYPE'; TABLE_ID = 'ASSET'; ATTRIBUTE_NM = 'COMPONENT_TYPE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'ASSET'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                  TABLE_ATTR_ID = 'ASSET_COMPONENT'; TABLE_ID = 'ASSET'; ATTRIBUTE_NM = 'COMPONENT'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'ASSET'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                  TABLE_ATTR_ID = 'ASSET_PRIMARYKEY'; TABLE_ID = 'ASSET'; ATTRIBUTE_NM = 'PRIMARYKEY'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'PAM_ASSET_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;

                                  if ('&adj_measure_table' ne '')
                                     then do;
                                            TABLE_ATTR_ID = 'TAGS_COMPONENT_GROUP'; TABLE_ID = 'TAGS'; ATTRIBUTE_NM = 'COMPONENT_GROUP'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'EVENT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'TAGS_COMPONENT_TYPE'; TABLE_ID = 'TAGS'; ATTRIBUTE_NM = 'COMPONENT_TYPE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'TAGS'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'TAGS_COMPONENT'; TABLE_ID = 'TAGS'; ATTRIBUTE_NM = 'COMPONENT'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'TAGS'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'TAGS_KEY_GROUP'; TABLE_ID = 'TAGS'; ATTRIBUTE_NM = 'KEY_GROUP'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'ASSET'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'TAGS_PRIMARYKEY'; TABLE_ID = 'TAGS'; ATTRIBUTE_NM = 'PRIMARYKEY'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'PAM_ASSET_ID TAG_ID MEASURE_DTTM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'TAGS_FOREIGNKEY'; TABLE_ID = 'TAGS'; ATTRIBUTE_NM = 'FOREIGNKEY'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'PAM_ASSET_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'TAGS_TRANSPOSE'; TABLE_ID = 'TAGS'; ATTRIBUTE_NM = 'TRANSPOSE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'TAGS_INTERPOLATE'; TABLE_ID = 'TAGS'; ATTRIBUTE_NM = 'INTERPOLATE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                          end;
                                  if ('&adj_event_measure_table' ne '')
                                     then do;
                                            TABLE_ATTR_ID = 'EVENTS_COMPONENT_GROUP'; TABLE_ID = 'EVENTS'; ATTRIBUTE_NM = 'COMPONENT_GROUP'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'EVENT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'EVENTS_COMPONENT_TYPE'; TABLE_ID = 'EVENTS'; ATTRIBUTE_NM = 'COMPONENT_TYPE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'EVENTS'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'EVENTS_COMPONENT'; TABLE_ID = 'EVENTS'; ATTRIBUTE_NM = 'COMPONENT'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'EVENTS'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'EVENTS_KEY_GROUP'; TABLE_ID = 'EVENTS'; ATTRIBUTE_NM = 'KEY_GROUP'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'ASSET'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'EVENTS_PRIMARYKEY'; TABLE_ID = 'EVENTS'; ATTRIBUTE_NM = 'PRIMARYKEY'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'PAM_ASSET_ID EVENT_ID EVENT_DTTM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'EVENTS_FOREIGNKEY'; TABLE_ID = 'EVENTS'; ATTRIBUTE_NM = 'FOREIGNKEY'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'PAM_ASSET_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLE_ATTR_ID = 'EVENTS_TRANSPOSE'; TABLE_ID = 'EVENTS'; ATTRIBUTE_NM = 'TRANSPOSE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                          end;
                           end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating TABLE_ATTRIBUTES."
                 ok_msg = " Step &step - NOTE: Table TABLE_ATTRIBUTES created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the TABLE_META table...";

            ds2.runDS2 result=res status=rc / 
                program = "data &inp_caslib..TABLE_META / overwrite=yes;
                                dcl char(144)  TABLE_ID;
                                dcl char(400)  CREATED_BY_NM;
                                dcl double     CREATION_DTTM having format datetime25.6;
                                dcl char(400)  MODIFIED_BY_NM;
                                dcl double     MODIFIED_DTTM having format datetime25.6;
                                dcl char(4000) TABLE_DESC;
                                dcl char(1000) TABLE_LABEL_TXT;
                                dcl double     LAST_DATA_DTTM having format 20.;
                                dcl char(400)  TABLE_NM;
                                dcl char(400)  SOLUTION_CD;
                                dcl char(2000) TABLE_URL_TXT;

                           method run();
                                  SOLUTION_CD = 'APA'; TABLE_ID = 'ASSET'; TABLE_NM = 'ASSET'; TABLE_DESC = '&TABLE_DEVICES_label'; TABLE_LABEL_TXT = '&TABLE_DEVICES_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;

                                  if ('&adj_measure_table' ne '')
                                     then do;
                                            SOLUTION_CD = 'APA'; TABLE_ID = 'TAGS'; TABLE_NM = 'TAGS'; TABLE_DESC = '&TABLE_MEASURES_label'; TABLE_LABEL_TXT = '&TABLE_MEASURES_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                          end;
                                  if ('&adj_event_measure_table' ne '')
                                     then do;
                                            SOLUTION_CD = 'APA'; TABLE_ID = 'EVENTS'; TABLE_NM = 'EVENTS'; TABLE_DESC = '&TABLE_EVENT_MEASURES_label'; TABLE_LABEL_TXT = '&TABLE_EVENT_MEASURES_label'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                          end;
                           end;
                           enddata;";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating TABLE_META."
                 ok_msg = " Step &step - NOTE: Table TABLE_META created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the TABLE_META_REFRESH_DATES table...";

            fedsql.execdirect result=res status=rc / 
                   query="create table &inp_caslib..TABLE_META_REFRESH_DATES {options replace=true} 
                                (TABLE_ID        char(144),
                                 CREATED_BY_NM   char(400),
                                 CREATION_DTTM   double having format datetime25.6,
                                 MODIFIED_BY_NM  char(400),
                                 MODIFIED_DTTM   double having format datetime25.6,
                                 TABLE_DESC      char(4000),
                                 TABLE_LABEL_TXT char(1000),
                                 LAST_DATA_DTTM  double having format datetime25.6,
                                 TABLE_NM        char(400),
                                 SOLUTION_CD     char(400),
                                 TABLE_URL_TXT   char(400))";

            if ("&adj_measure_table" != '')
               then do;
                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..TABLE_META_REFRESH_DATES / overwrite=yes;
                                          dcl char(144)  TABLE_ID;
                                          dcl char(400)  CREATED_BY_NM;
                                          dcl double     CREATION_DTTM having format datetime25.6;
                                          dcl char(400)  MODIFIED_BY_NM;
                                          dcl double     MODIFIED_DTTM having format datetime25.6;
                                          dcl char(4000) TABLE_DESC;
                                          dcl char(1000) TABLE_LABEL_TXT;
                                          dcl double     LAST_DATA_DTTM having format datetime25.6;
                                          dcl char(400)  TABLE_NM;
                                          dcl char(400)  SOLUTION_CD;
                                          dcl char(400)  TABLE_URL_TXT;

                           method run();
                                  set {select TABLE_ID,
                                              CREATED_BY_NM,
                                              CAST(CREATION_DTTM as double) as CREATION_DTTM,
                                              MODIFIED_BY_NM,
                                              CAST(MODIFIED_DTTM as double) as MODIFIED_DTTM,
                                              TABLE_DESC,
                                              TABLE_LABEL_TXT,
                                              CAST(LAST_DATA_DTTM as double) as LAST_DATA_DTTM,
                                              TABLE_NM,
                                              SOLUTION_CD,
                                              TABLE_URL_TXT
                                         from &inp_caslib..TABLE_META_REFRESH_DATES}
                                      {select 'TAGS' as TABLE_ID, 
                                              '&sysuserid' as CREATED_BY_NM,
                                              datetime() as CREATION_DTTM,
                                              '&sysuserid' as MODIFIED_BY_NM, 
                                              datetime() as MODIFIED_DTTM,
                                              'Measures' as TABLE_DESC,
                                              'Measures' as TABLE_LABEL_TXT,
                                              CAST(MAX(MEASURE_DTTM) as double) as LAST_DATA_DTTM,
                                              'TAGS' as TABLE_NM,
                                              'APA' as SOLUTION_CD,
                                              '' as TABLE_URL_TXT
                                         from &inp_caslib..PAM_ASSET_LOC_MEASURE_FACT};
                           end;
                           enddata;";
                    end;


            if ("&adj_event_measure_table" != '')
               then do;
                      ds2.runDS2 result=res status=rc / 
                          program = "data &inp_caslib..TABLE_META_REFRESH_DATES / overwrite=yes;
                                          dcl char(144)  TABLE_ID;
                                          dcl char(400)  CREATED_BY_NM;
                                          dcl double     CREATION_DTTM having format datetime25.6;
                                          dcl char(400)  MODIFIED_BY_NM;
                                          dcl double     MODIFIED_DTTM having format datetime25.6;
                                          dcl char(4000) TABLE_DESC;
                                          dcl char(1000) TABLE_LABEL_TXT;
                                          dcl double     LAST_DATA_DTTM having format datetime25.6;
                                          dcl char(400)  TABLE_NM;
                                          dcl char(400)  SOLUTION_CD;
                                          dcl char(400)  TABLE_URL_TXT;

                           method run();
                                  set {select TABLE_ID,
                                              CREATED_BY_NM,
                                              CAST(CREATION_DTTM as double) as CREATION_DTTM,
                                              MODIFIED_BY_NM,
                                              CAST(MODIFIED_DTTM as double) as MODIFIED_DTTM,
                                              TABLE_DESC,
                                              TABLE_LABEL_TXT,
                                              CAST(LAST_DATA_DTTM as double) as LAST_DATA_DTTM,
                                              TABLE_NM,
                                              SOLUTION_CD,
                                              TABLE_URL_TXT
                                         from &inp_caslib..TABLE_META_REFRESH_DATES}
                                      {select 'EVENTS' as TABLE_ID, 
                                              '&sysuserid' as CREATED_BY_NM,
                                              CAST(datetime() as double) as CREATION_DTTM,
                                              '&sysuserid' as MODIFIED_BY_NM, 
                                              CAST(datetime() as double) as MODIFIED_DTTM,
                                              'Measures' as TABLE_DESC,
                                              'Measures' as TABLE_LABEL_TXT,
                                              CAST(MAX(EVENT_DTTM) as double) as LAST_DATA_DTTM,
                                              'EVENTS' as TABLE_NM,
                                              'APA' as SOLUTION_CD,
                                              '' as TABLE_URL_TXT
                                         from &inp_caslib..PAM_ASSET_LOC_EVENT_FACT};
                           end;
                           enddata;";
                    end;
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating TABLE_META_REFRESH_DATES."
                 ok_msg = " Step &step - NOTE: Table TABLE_META_REFRESH_DATES created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the TABLECOLUMN_META table...";

            datastep.runcode result=res status=rc / 
                     code = "data &inp_caslib..TABLECOLUMN_META;
                                  length COLUMN_ID           $144;
                                  length CREATED_BY_NM       $400;
                                  length CREATION_DTTM       8;
                                  format CREATION_DTTM       datetime25.6;
                                  length MODIFIED_BY_NM      $400;
                                  length MODIFIED_DTTM       8;
                                  format MODIFIED_DTTM       datetime25.6;
                                  length COLUMN_DATA_TYPE_CD 8;
                                  length COLUMN_DESC         $4000;
                                  length COLUMN_FMT_NM       $400;
                                  length COLUMN_LABEL_TXT    $1000;
                                  length COLUMN_NM           $400;
                                  length TABLE_ID            $144;
                                  length SOLUTION_CD         $400;

                                  if (_n_ = 1)
                                     then do;
                                            COLUMN_ID = 'PAM_ASSET_ID'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Asset ID'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_DEVICE_ID_label'; COLUMN_NM = 'PAM_ASSET_ID'; TABLE_ID = 'ASSET'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            if ('&adj_measure_table' ne '')
                                               then do;
						                              COLUMN_ID = 'TAG_ID'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Tag Id'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_SENSOR_ID_label'; COLUMN_NM = 'TAG_ID'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'TAG_COLUMN_NM'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Tag Column Name'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_TRANS_COLUMN_label'; COLUMN_NM = 'TAG_COLUMN_NM'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'TAG_DEF_AGGREG_METHOD_CD'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Default Aggregation Method'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_AGGREG_label'; COLUMN_NM = 'TAG_DEF_AGGREG_METHOD_CD'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'TAG_DEF_INTERPOL_METHOD_CD'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Default Interpolation Method'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_INTER_label'; COLUMN_NM = 'TAG_DEF_INTERPOL_METHOD_CD'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'MEASURE_DTTM'; COLUMN_DATA_TYPE_CD = 6; COLUMN_DESC = 'Datetime'; COLUMN_FMT_NM = 'DATETIME.'; COLUMN_LABEL_TXT = '&TABLE_MEASURE_DTTM_label'; COLUMN_NM = 'MEASURE_DTTM'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'MEASURE_DATE'; COLUMN_DATA_TYPE_CD = 5; COLUMN_DESC = 'Measurement Date'; COLUMN_FMT_NM = 'NLDATE10.'; COLUMN_LABEL_TXT = '&TABLE_MEASURE_DATE_label'; COLUMN_NM = 'MEASURE_DATE'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'MEASURE_TIME'; COLUMN_DATA_TYPE_CD = 7; COLUMN_DESC = 'Measurement Time'; COLUMN_FMT_NM = 'NLTIME8.'; COLUMN_LABEL_TXT = '&TABLE_MEASURE_TIME_label'; COLUMN_NM = 'MEASURE_TIME'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'LOC_MEASURE_VALUE'; COLUMN_DATA_TYPE_CD = 4; COLUMN_DESC = 'Value'; COLUMN_FMT_NM = 'F9.4'; COLUMN_LABEL_TXT = '&TABLE_MEASURE_VALUE_label'; COLUMN_NM = 'LOC_MEASURE_VALUE'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'TAGS.PAM_ASSET_ID'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Asset ID'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_DEVICE_ID_label'; COLUMN_NM = 'PAM_ASSET_ID'; TABLE_ID = 'TAGS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                            end;
					                        if ('&adj_event_measure_table' ne '')
					                           then do;
					  	                              COLUMN_ID = 'EVENT_ID'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Event Id'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_EVENT_ID_label'; COLUMN_NM = 'EVENT_ID'; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
					  	                              COLUMN_ID = 'EVENT_COLUMN_NM'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Event Column Name'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_TRANS_COLUMN_label'; COLUMN_NM = 'EVENT_COLUMN_NM'; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'EVENT_DEF_AGGREG_METHOD_CD'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Event Default Aggregation Method'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_AGGREG_label'; COLUMN_NM = 'EVENT_DEF_AGGREG_METHOD_CD'; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'EVENT_DTTM'; COLUMN_DATA_TYPE_CD = 6; COLUMN_DESC = 'Datetime'; COLUMN_FMT_NM = 'DATETIME.'; COLUMN_LABEL_TXT = '&TABLE_EVENT_DTTM_label'; COLUMN_NM = 'EVENT_DTTM'; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'EVENT_DATE'; COLUMN_DATA_TYPE_CD = 5; COLUMN_DESC = 'Measurement Date'; COLUMN_FMT_NM = 'NLDATE10.'; COLUMN_LABEL_TXT = '&TABLE_EVENT_DATE_label'; COLUMN_NM = 'EVENT_DATE'; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'EVENT_TIME'; COLUMN_DATA_TYPE_CD = 7; COLUMN_DESC = 'Measurement Time'; COLUMN_FMT_NM = 'NLTIME8.'; COLUMN_LABEL_TXT = '&TABLE_EVENT_TIME_label'; COLUMN_NM = 'EVENT_TIME'; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'EVENT_VALUE'; COLUMN_DATA_TYPE_CD = 4; COLUMN_DESC = 'Value'; COLUMN_FMT_NM = 'F9.4'; COLUMN_LABEL_TXT = '&TABLE_EVENT_VALUE_label'; COLUMN_NM = 'EVENT_VALUE'; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                              COLUMN_ID = 'EVENTS.PAM_ASSET_ID'; COLUMN_DATA_TYPE_CD = 2; COLUMN_DESC = 'Asset ID'; COLUMN_FMT_NM = ''; COLUMN_LABEL_TXT = '&TABLE_DEVICE_ID_label'; COLUMN_NM = 'PAM_ASSET_ID'; TABLE_ID = 'EVENTS'; SOLUTION_CD = 'APA'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
						                            end;
                                            if (
                                                (&extra_dev_cols > 0) or
                                                (&extra_eve_cols > 0) or
                                                (&extra_evm_cols > 0) or
                                                (&extra_mea_cols > 0) or
                                                (&extra_sen_cols > 0)
                                               )        
                                               then do until (LastObs);
                                                       set casuser.table_colmeta_extra_cols end=lastobs;
                                                       output;
                                                    end;
                                          end;
                             run;"
                     single="YES";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating TABLECOLUMN_META."
                 ok_msg = " Step &step - NOTE: Table TABLECOLUMN_META created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Creating the TABLECOLUMN_ATTRIBUTES table...";

            datastep.runcode result=res status=rc / 
                     code = "data &inp_caslib..TABLECOLUMN_ATTRIBUTES;
                                  length TABLECOLUMN_ATTR_ID    $144;
                                  length CREATED_BY_NM          $400;
                                  length CREATION_DTTM          8;
                                  format CREATION_DTTM          datetime25.6;
                                  length MODIFIED_BY_NM         $400;
                                  length MODIFIED_DTTM          8;
                                  format MODIFIED_DTTM          datetime25.6;
                                  length ATTRIBUTE_DATA_TYPE_CD 8;
                                  length ATTRIBUTE_NM           $400;
                                  length COLUMN_ID              $144;
                                  length ATTRIBUTE_VAL          $2000;

                                  if (_n_ = 1)
                                     then do;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_FACT_TABLE'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_FACT_COLUMN'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_DIM_TABLE'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_DIM_COLUMN'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'PAM_ASSET_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_DIM_NAMECOLUMN'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_NAMECOLUMN'; ATTRIBUTE_VAL = 'PAM_ASSET_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_DIM_PRIMARYKEY'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_REQVAR'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_ID_COLUMN'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'ID_COLUMN'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                            TABLECOLUMN_ATTR_ID = 'PAM_ASSET_ID_JOIN_COLUMN'; COLUMN_ID = 'PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'JOIN_COLUMN'; ATTRIBUTE_VAL = '1'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
 
                                            if ('&adj_measure_table' ne '')
                                               then do;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_FACT_TABLE'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_FACT_COLUMN'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_DIM_COLUMN'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'TAG_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_DIM_NAMECOLUMN'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_NAMECOLUMN'; ATTRIBUTE_VAL = 'TAG_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_DIM_TABLE'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_TAG_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_DIM_PRIMARYKEY'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_JOIN_COLUMN'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'JOIN_COLUMN'; ATTRIBUTE_VAL = '1'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_ID_COLUMN'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'ID_COLUMN'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_LIMITVAR'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'LIMITVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_ID_REQVAR'; COLUMN_ID = 'TAG_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_COLUMN_NM_FACT_TABLE'; COLUMN_ID = 'TAG_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_COLUMN_NM_FACT_COLUMN'; COLUMN_ID = 'TAG_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_COLUMN_NM_DIM_TABLE'; COLUMN_ID = 'TAG_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_TAG_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_COLUMN_NM_DIM_COLUMN'; COLUMN_ID = 'TAG_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'TAG_COLUMN_NM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_COLUMN_NM_DIM_PRIMARYKEY'; COLUMN_ID = 'TAG_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_AGGREG_METH_FACT_TABLE'; COLUMN_ID = 'TAG_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_AGGREG_METH_FACT_COLUMN'; COLUMN_ID = 'TAG_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_AGGREG_METH_DIM_TABLE'; COLUMN_ID = 'TAG_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_TAG_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_AGGREG_METH_DIM_COLUMN'; COLUMN_ID = 'TAG_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'TAG_DEF_AGGREG_METHOD_CD'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_AGGREG_METH_DIM_PRIMARYKEY'; COLUMN_ID = 'TAG_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_INTERPOL_METH_FACT_TABLE'; COLUMN_ID = 'TAG_DEF_INTERPOL_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_INTERPOL_METH_FACT_COLUMN'; COLUMN_ID = 'TAG_DEF_INTERPOL_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_INTERPOL_METH_DIM_TABLE'; COLUMN_ID = 'TAG_DEF_INTERPOL_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_TAG_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_INTERPOL_METH_DIM_COLUMN'; COLUMN_ID = 'TAG_DEF_INTERPOL_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'TAG_DEF_INTERPOL_METHOD_CD'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAG_INTERPOL_METH_DIM_PRIMARYKEY'; COLUMN_ID = 'TAG_DEF_INTERPOL_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_TAG_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DTTM_FACT_TABLE'; COLUMN_ID = 'MEASURE_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DTTM_FACT_COLUMN'; COLUMN_ID = 'MEASURE_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'MEASURE_DTTM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DTTM_REQVAR'; COLUMN_ID = 'MEASURE_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DTTM_DEFAULT_VALUE'; COLUMN_ID = 'MEASURE_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DEFAULT_VALUE'; ATTRIBUTE_VAL = '30'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DTTM_MEASURE_DTTM'; COLUMN_ID = 'MEASURE_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MEASURE_DTTM'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DTTM_MANDATORY_VAR'; COLUMN_ID = 'MEASURE_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MANDATORY_VAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DATE_FACT_TABLE'; COLUMN_ID = 'MEASURE_DATE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DATE_FACT_COLUMN'; COLUMN_ID = 'MEASURE_DATE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_DATE_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_DATE_MEASURE_DATE'; COLUMN_ID = 'MEASURE_DATE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MEASURE_DATE'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_TIME_FACT_TABLE'; COLUMN_ID = 'MEASURE_TIME'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_TIME_FACT_COLUMN'; COLUMN_ID = 'MEASURE_TIME'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_TIME_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'MEASURE_TIME_MEASURE_TIME'; COLUMN_ID = 'MEASURE_TIME'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MEASURE_TIME'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'LOC_MEASURE_VALUE_FACT_TABLE'; COLUMN_ID = 'LOC_MEASURE_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'LOC_MEASURE_VALUE_FACT_COLUMN'; COLUMN_ID = 'LOC_MEASURE_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'LOC_MEASURE_VALUE'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'LOC_MEASURE_VALUE_REQVAR'; COLUMN_ID = 'LOC_MEASURE_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'LOC_MEASURE_VALUE_MEASURE_VALUE'; COLUMN_ID = 'LOC_MEASURE_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MEASURE_VALUE'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'LOC_MEASURE_VALUE_LIMITCOLUMN'; COLUMN_ID = 'LOC_MEASURE_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'LIMITCOLUMN'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAGS.PAM_ASSET_ID_FACT_TABLE'; COLUMN_ID = 'TAGS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_MEASURE_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAGS.PAM_ASSET_ID_FACT_COLUMN'; COLUMN_ID = 'TAGS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAGS.PAM_ASSET_ID_DIM_TABLE'; COLUMN_ID = 'TAGS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAGS.PAM_ASSET_ID_DIM_COLUMN'; COLUMN_ID = 'TAGS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'PAM_ASSET_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAGS.PAM_ASSET_ID_DIM_PRIMARYKEY'; COLUMN_ID = 'TAGS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAGS.PAM_ASSET_ID_REQVAR'; COLUMN_ID = 'TAGS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'TAGS.PAM_ASSET_ID_TRANSPOSE_VAR'; COLUMN_ID = 'TAGS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'TRANSPOSE_VAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                    end;
                                            if ('&adj_event_measure_table' ne '')
                                               then do;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_FACT_TABLE'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_FACT_COLUMN'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_DIM_COLUMN'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'EVENT_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_DIM_NAMECOLUMN'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_NAMECOLUMN'; ATTRIBUTE_VAL = 'EVENT_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_DIM_TABLE'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_DIM_PRIMARYKEY'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_JOIN_COLUMN'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'JOIN_COLUMN'; ATTRIBUTE_VAL = '1'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_ID_COLUMN'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'ID_COLUMN'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_LIMITVAR'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'LIMITVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_ID_REQVAR'; COLUMN_ID = 'EVENT_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_COLUMN_NM_FACT_TABLE'; COLUMN_ID = 'EVENT_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_COLUMN_NM_FACT_COLUMN'; COLUMN_ID = 'EVENT_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_COLUMN_NM_DIM_TABLE'; COLUMN_ID = 'EVENT_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_COLUMN_NM_DIM_COLUMN'; COLUMN_ID = 'EVENT_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'EVENT_COLUMN_NM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_COLUMN_NM_DIM_PRIMARYKEY'; COLUMN_ID = 'EVENT_COLUMN_NM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_AGGREG_METH_FACT_TABLE'; COLUMN_ID = 'EVENT_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_AGGREG_METH_FACT_COLUMN'; COLUMN_ID = 'EVENT_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_AGGREG_METH_DIM_TABLE'; COLUMN_ID = 'EVENT_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_AGGREG_METH_DIM_COLUMN'; COLUMN_ID = 'EVENT_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'EVENT_DEF_AGGREG_METHOD_CD'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_AGGREG_METH_DIM_PRIMARYKEY'; COLUMN_ID = 'EVENT_DEF_AGGREG_METHOD_CD'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_EVENT_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DTTM_FACT_TABLE'; COLUMN_ID = 'EVENT_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DTTM_FACT_COLUMN'; COLUMN_ID = 'EVENT_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'EVENT_DTTM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DTTM_REQVAR'; COLUMN_ID = 'EVENT_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DTTM_DEFAULT_VALUE'; COLUMN_ID = 'EVENT_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DEFAULT_VALUE'; ATTRIBUTE_VAL = '30'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DTTM_MEASURE_DTTM'; COLUMN_ID = 'EVENT_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MEASURE_DTTM'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DTTM_MANDATORY_VAR'; COLUMN_ID = 'EVENT_DTTM'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MANDATORY_VAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DATE_FACT_TABLE'; COLUMN_ID = 'EVENT_DATE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DATE_FACT_COLUMN'; COLUMN_ID = 'EVENT_DATE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_DATE_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_DATE_MEASURE_DATE'; COLUMN_ID = 'EVENT_DATE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MEASURE_DATE'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_TIME_FACT_TABLE'; COLUMN_ID = 'EVENT_TIME'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_TIME_FACT_COLUMN'; COLUMN_ID = 'EVENT_TIME'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_TIME_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_TIME_MEASURE_TIME'; COLUMN_ID = 'EVENT_TIME'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MEASURE_TIME'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_VALUE_FACT_TABLE'; COLUMN_ID = 'EVENT_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_VALUE_FACT_COLUMN'; COLUMN_ID = 'EVENT_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'EVENT_VALUE'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_VALUE_REQVAR'; COLUMN_ID = 'EVENT_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_VALUE_MEASURE_VALUE'; COLUMN_ID = 'EVENT_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'MEASURE_VALUE'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVENT_VALUE_LIMITCOLUMN'; COLUMN_ID = 'EVENT_VALUE'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'LIMITCOLUMN'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVTS.PAM_ASSET_ID_FACT_TABLE'; COLUMN_ID = 'EVENTS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_LOC_EVENT_FACT'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVTS.PAM_ASSET_ID_FACT_COLUMN'; COLUMN_ID = 'EVENTS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'FACT_COLUMN'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVTS.PAM_ASSET_ID_DIM_TABLE'; COLUMN_ID = 'EVENTS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_TABLE'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVTS.PAM_ASSET_ID_DIM_COLUMN'; COLUMN_ID = 'EVENTS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_COLUMN'; ATTRIBUTE_VAL = 'PAM_ASSET_ID'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVTS.PAM_ASSET_ID_DIM_PRIMARYKEY'; COLUMN_ID = 'EVENTS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'DIM_PRIMARYKEY'; ATTRIBUTE_VAL = 'PAM_ASSET_DIM_RK'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVTS.PAM_ASSET_ID_REQVAR'; COLUMN_ID = 'EVENTS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'REQVAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                      TABLECOLUMN_ATTR_ID = 'EVTS.PAM_ASSET_ID_TRANSPOSE_VAR'; COLUMN_ID = 'EVENTS.PAM_ASSET_ID'; ATTRIBUTE_DATA_TYPE_CD = 2; ATTRIBUTE_NM = 'TRANSPOSE_VAR'; ATTRIBUTE_VAL = 'Y'; CREATED_BY_NM = '&sysuserid'; CREATION_DTTM = datetime(); MODIFIED_BY_NM = '&sysuserid'; MODIFIED_DTTM = datetime(); output;
                                                    end;
                                            if (
                                                (&extra_dev_cols > 0) or
                                                (&extra_eve_cols > 0) or
                                                (&extra_evm_cols > 0) or
                                                (&extra_mea_cols > 0) or
                                                (&extra_sen_cols > 0)
                                               )        
                                               then do until (LastObs);
                                                       set casuser.table_colattr_extra_cols end=lastobs;
                                                       output;
                                                    end;

                                          end;
                             run;"
                     single="YES";
            rc.status = '';

            AIot.ReturnCodeHandler result=res / 
                 ret_code = rc
                 err_msg = " Step &step - ERROR: An error occurred while creating TABLECOLUMN_ATTRIBUTES."
                 ok_msg = " Step &step - NOTE: Table TABLECOLUMN_ATTRIBUTES created successfully.";

            if (rc.severity > 1)
               then do;
                      call symputx('return_code', 8);
                      exit(0);
                    end;

            run;
       quit;

       libname casuser clear;
%mend aiot_create_metadata;

%macro aiot_save_output;
       /*=========================================================================================*/
       /* Save output tables                                                                      */
       /*=========================================================================================*/
       %if (&bkp_caslib eq)
           %then %do;
                    %put %sysfunc(date(),date9.) %sysfunc(time(),tod8.) Step &step - NOTE: Output caslib not specified. Tables will not be saved.;
                    %return;
                 %end;

       proc cas;
            session "&_sessref_";

            /*=========================================================================================*/
            /* Query CASLIB to save output data                                                        */
            /*=========================================================================================*/
            AIot.CurrentTime result=res / message = " Step &step - NOTE: Querying caslib "||upcase("&bkp_caslib")||"...";

            queryCaslib result=res status=rc /
                        caslib="&bkp_caslib";

            if (res[1] = FALSE)
               then do;
                      AIot.CurrentTime result=res / message = " Step &step - ERROR: The CAS library "||upcase("&bkp_caslib")||" is not assigned.";
                      call symputx('return_code', 8);
                      exit;
                    end;
               else do;
                      table.caslibInfo result=res status=rc /
                            caslib="&bkp_caslib";

                      /*=========================================================================================*/
                      /* An ESP library cannot be used to save the output tables                                 */
                      /*=========================================================================================*/
                      if (res.CASLibInfo[1].Type == 'ESP')
                         then do;
                                AIot.CurrentTime result=res / message = " Step &step - ERROR: The CAS library "||upcase("&bkp_caslib")||" is a read-only library.";
                                call symputx('return_code', 8);
                                exit;
                              end;

                      /*=========================================================================================*/
                      /* Save tables to output caslib. NOTE: the list might not include all the ones shown below.*/
                      /*=========================================================================================*/
                      TableList = {"FILTER_ATTRIBUTE",
                                   "FILTER_ATTRIBUTE_GROUP",
                                   "FILTER_ATTRIBUTE_TREE",
                                   "LOCALIZATION_GROUP_DATASELECTION",
                                   "LOCALIZATION_GROUP_MARTMETADATA",
                                   "LOCALIZATION_VALUE_DATASELECTION",
                                   "LOCALIZATION_VALUE_MARTMETADATA",
                                   "PAM_ASSET_DIM", 
                                   "PAM_ASSET_LOC_EVENT_FACT", 
                                   "PAM_ASSET_LOC_MEASURE_FACT", 
                                   "PAM_EVENT_DIM", 
                                   "PAM_FACT_EVENT_STATS", 
                                   "PAM_FACT_TAG_MEASURES_STATS", 
                                   "PAM_TAG_DIM", 
                                   "PREDEFINED_DATE",
                                   "TABLE_ATTRIBUTES",
                                   "TABLE_META",
                                   "TABLE_META_REFRESH_DATES",
                                   "TABLECOLUMN_ATTRIBUTES",
                                   "TABLECOLUMN_META"};

                      if ("&out_replace" == 'Y')
                         then replace = TRUE;
                         else replace = FALSE;

                      AIot.CurrentTime result=res / message = " Step &step - NOTE: Saving tables in caslib "||upcase("&bkp_caslib")||"...";

                      do i = 1 to dim(TableList);
                         table.tableExists result=res status=rc /
                               caslib="&inp_caslib"
                               name=TableList[i];
                         rc.status = '';

                         if (res.exists == 0)
                            then continue;

                         table.save result=res status=rc / 
                               caslib="&bkp_caslib"
                               name=TableList[i]
                               replace=replace
                               table={
                                      caslib="&inp_caslib" 
                                      name=TableList[i]
                                     };
                         rc.status = '';

                         AIot.ReturnCodeHandler result=res / 
                              ret_code = rc
                              err_msg = " Step &step - ERROR: An error occurred while saving "||TableList[i]||"."
                              ok_msg = " Step &step - NOTE: Table "||TableList[i]||" saved successfully.";

                         if (rc.severity > 1)
                            then do;
                                   call symputx('return_code', 8);
                                   exit(0);
                                 end;
                      end;
                    end;                     
            run;
       quit;
%mend aiot_save_output;

%macro aiot_promote;
       /*=========================================================================================*/
       /* Promote output tables to AIoT mart                                                      */
       /*=========================================================================================*/
       proc cas;
            session "&_sessref_";

            /*=========================================================================================*/
            /* Query CASLIB to promote output data                                                     */
            /*=========================================================================================*/
            AIot.CurrentTime result=res / message = " Step &step - NOTE: Querying caslib "||upcase("&aiot_caslib")||"...";

            queryCaslib result=res status=rc /
                        caslib="&aiot_caslib";

            if (res[1] = FALSE)
               then do;
                      AIot.CurrentTime result=res / message = " Step &step - ERROR: The CAS library "||upcase("&aiot_caslib")||" is not assigned.";
                      call symputx('return_code', 8);
                      exit;
                    end;
               else do;
                      /*=========================================================================================*/
                      /* Promote tables to AIoT. NOTE: the list might not include all the ones shown below.      */
                      /*=========================================================================================*/
                      TableList = {"FILTER_ATTRIBUTE",
                                   "FILTER_ATTRIBUTE_GROUP",
                                   "FILTER_ATTRIBUTE_TREE",
                                   "LOCALIZATION_GROUP_DATASELECTION",
                                   "LOCALIZATION_GROUP_MARTMETADATA",
                                   "LOCALIZATION_VALUE_DATASELECTION",
                                   "LOCALIZATION_VALUE_MARTMETADATA",
                                   "PAM_ASSET_DIM", 
                                   "PAM_ASSET_LOC_EVENT_FACT", 
                                   "PAM_ASSET_LOC_MEASURE_FACT", 
                                   "PAM_EVENT_DIM", 
                                   "PAM_FACT_EVENT_STATS", 
                                   "PAM_FACT_TAG_MEASURES_STATS", 
                                   "PAM_TAG_DIM", 
                                   "PREDEFINED_DATE",
                                   "TABLE_ATTRIBUTES",
                                   "TABLE_META",
                                   "TABLE_META_REFRESH_DATES",
                                   "TABLECOLUMN_ATTRIBUTES",
                                   "TABLECOLUMN_META"};

                      AIot.CurrentTime result=res / message = " Step &step - NOTE: Promoting tables to caslib "||upcase("&aiot_caslib")||"...";

                      do i = 1 to dim(TableList);
                         table.tableExists result=checktabs status=rc /
                               caslib="&inp_caslib"
                               name=TableList[i];
                         rc.status = '';

                         table.dropTable result=res / caslib="&aiot_caslib" table=TableList[i] quiet=TRUE;

                         if (checktabs.exists == 0)
                            then continue;

                         table.promote result=res status=rc /
                               caslib="&inp_caslib"
                               drop=TRUE
                               name=TableList[i]
                               targetLib="&aiot_caslib";
                         rc.status = '';

                         AIot.ReturnCodeHandler result=res / 
                              ret_code = rc
                              err_msg = " Step &step - ERROR: An error occurred while promoting "||TableList[i]||"."
                              ok_msg = " Step &step - NOTE: Table "||TableList[i]||" promoted successfully.";

                         if (rc.severity > 1)
                            then do;
                                   call symputx('return_code', 8);
                                   exit(0);
                                 end;
                      end;
                    end;

                 if (rc.severity = 0)
                    then do;
                           table.dropTable result=res / caslib="&aiot_caslib" table="RECREATE_METADATA" quiet=TRUE;
                           table.dropTable result=res / caslib="&aiot_caslib" table="REFRESH_DATES" quiet=TRUE;

                           datastep.runcode result=res status=rc / 
                                    code = "data aiotclib.RECREATE_METADATA (caslib=&aiot_caslib promote=yes) aiotclib.REFRESH_DATES (caslib=&aiot_caslib promote=yes);
                                                 ETL='QAS';
                                                 STATUS='SUCCESS';
                                            run;"
                                    single="YES";

                           if (rc.severity > 1)
                              then do;
                                     call symputx('return_code', 8);
                                     exit(0);
                                   end;
                         end;
            run;
       quit;
%mend aiot_promote;

%macro aiot_cleanup;
       /*=========================================================================================*/
       /* Cleaning up                                                                             */
       /*=========================================================================================*/
       proc cas; 
            session "&_sessref_";

            AIot.CurrentTime result=res / message = " Step &step - NOTE: Cleaning up...";

            table.dropTable result=res / caslib="&inp_caslib" table="&adj_measure_table" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="&adj_event_table" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="&adj_event_measure_table" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="&adj_event_hierarchy_table" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="&adj_device_table" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="&adj_device_hierarchy_table" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="&adj_sensor_table" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="&adj_sensor_hierarchy_table" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="lineage" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="&adj_measure_table"||"_COLINFO" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="MEASURE_COLUMN_NAMES" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="PAM_TAG_DIM_TMP" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="PAM_ASSET_DIM_TMP" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="PAM_EVENT_DIM_TMP" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="CASDS2_SQL_EB087967_1" quiet=TRUE;
            table.dropTable result=res / caslib="&inp_caslib" table="CASDS2_SQL_EB087967_2" quiet=TRUE;

            run;
       quit;
%mend aiot_cleanup;

/*=========================================================================================*/
/* Main                                                                                    */
/*=========================================================================================*/
%macro aiot_main(parameter_file,
                 print_parameters,
                 first_step,
                 input_caslib,
                 backup_caslib,
                 replace_backup,
                 load_pregen,
                 debug);

       %global param_file step inp_caslib bkp_caslib out_replace load_pre_gen return_code print_params;
       %let param_file = &parameter_file;
       %let print_params = %upcase(&print_parameters);
       %let step = &first_step;
       %let inp_caslib = &input_caslib;
       %let bkp_caslib = &backup_caslib;
       %let out_replace = %upcase(&replace_backup);
       %let load_pre_gen = %upcase(&load_pregen);
       %let return_code = 0;

       /*=========================================================================================*/
       /* If loading a set of pre-existing tables, there is no restart.                           */
       /*=========================================================================================*/
       %if (&load_pre_gen = Y)
           %then %let step = 1;

       %if (%upcase(&debug) = Y)
           %then %do;
                    options notes source source2 quotelenmax mprint mprintnest symbolgen mlogic mlogicnest;
                 %end;
           %else %do;
                    options nonotes nosource nosource2 noquotelenmax nomprint nomprintnest nosymbolgen nomlogic nomlogicnest;
                 %end;

       proc cas; 
            session "&_sessref_";

            function current_time();
                     return(put(date(),date9.)||" "||put(time(),tod8.));
            end;

            start_time = current_time();
            call symputx('start_time', start_time);

            print "--------------------------------------------------------------------------------------------------------------";
            print current_time() " NOTE: Starting program from step &step.";
            print "--------------------------------------------------------------------------------------------------------------";

            run;
       quit;

       %aiot_read_parameters;

       %if (&return_code = 0)
           %then %aiot_set_up_environment;

       %if ((&step = 1) & (&return_code = 0))
           %then %do; 
                    %aiot_load_data;
                    %let step = %eval(&step + 1);
                    /*=========================================================================================*/
                    /* If loading a set of pre-existing tables, save (optional), promote and stop              */
                    /*=========================================================================================*/
                    %if ((&load_pre_gen = Y) & (&return_code = 0)) 
                        %then %do;
                                 %aiot_save_output;
                                 %let step = %eval(&step + 1);
                                 %aiot_promote;
                                 %let step = %eval(&step + 1);
                                 %if (&return_code = 0)
                                     %then %do;
                                              %if (%upcase(&debug) = N)
                                                  %then %aiot_cleanup;
                                           %end;
                                 %let step = -1;
                              %end;
                 %end;
       %if ((&step = 2) & (&return_code = 0))
           %then %do;
                    %aiot_datetime_type_conversion(&adj_measure_table);
                    %aiot_datetime_type_conversion(&adj_event_measure_table);
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 3) & (&return_code = 0))
           %then %do; 
                    %aiot_pam_asset_dim;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 4) & (&return_code = 0))
           %then %do;
                    %aiot_pam_tag_dim;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 5) & (&return_code = 0))
           %then %do;
                    %aiot_pam_asset_loc_measure_fact;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 6) & (&return_code = 0))
           %then %do;
                    %aiot_pam_fact_tag_measures_stat;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 7) & (&return_code = 0))
           %then %do; 
                    %aiot_pam_event_dim;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 8) & (&return_code = 0))
           %then %do; 
                    %aiot_pam_asset_loc_event_fact;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 9) & (&return_code = 0))
           %then %do; 
                    %aiot_pam_fact_event_stats;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 10) & (&return_code = 0))
           %then %do;
                    %aiot_process_hierarchies(&adj_device_hierarchy_table, DEVICE);
                    %aiot_process_hierarchies(&adj_event_hierarchy_table, EVENT);
                    %aiot_process_hierarchies(&adj_sensor_hierarchy_table, SENSOR);
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 11) & (&return_code = 0))
           %then %do;
                    %aiot_predefined_date;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 12) & (&return_code = 0))
           %then %do;
                    %aiot_create_metadata;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 13) & (&return_code = 0))
           %then %do;
                    %aiot_save_output;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 14) & (&return_code = 0))
           %then %do;
                    %aiot_promote;
                    %let step = %eval(&step + 1);
                 %end;
       %if ((&step = 15) & (&return_code = 0))
           %then %do;
                    %if (%upcase(&debug) = N)
                        %then %do;
                                 %aiot_cleanup;
                                 %let step = %eval(&step + 1);
                              %end;
                 %end;

       proc cas; 
            session "&_sessref_";

            function current_time();
                     return(put(date(),date9.)||" "||put(time(),tod8.));
            end;

            print "--------------------------------------------------------------------------------------------------------------";
            print "Start time : &start_time";
            print "End time   : " current_time();
            print "--------------------------------------------------------------------------------------------------------------";

            run;
       quit;
%mend aiot_main;

