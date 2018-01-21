Accidents = LOAD '/usr/local/Accidents0515.csv' USING PigStorage(',') AS (Accident_Index:chararray,Location_Easting_OSGR:long,Location_Northing_OSGR:long,Longitude:double,Latitude:double,Police_Force:int,Accident_Severity:int,Number_of_Vehicles:int,Number_of_Casualties:int,Date:chararray,Day_of_Week:int,Time:chararray,Local_Authority_District:chararray,Local_Authority_Highway:chararray,First_Road_Class:int,First_Road_Number:int,Road_Type:int,Speed_limit:int,Junction_Detail:int,Junction_Control:int,Second_Road_Class:int,Second_Road_Number:int,Pedestrian_Crossing_Human_Control:int,Pedestrian_Crossing_Physical_Facilities:int,Light_Conditions:int,Weather_Conditions:int,Road_Surface_Conditions:int,Special_Conditions_at_Site:int,Carriageway_Hazards:int,Urban_or_Rural_Area:int,Did_Police_Officer_Attend_Scene_of_Accident:int,LSOA_of_Accident_Location:chararray);

GROUP_BY_DATE = GROUP Accidents by Date;

GROUP_SUM = FOREACH GROUP_BY_DATE GENERATE FLATTEN(group), SUM(Accidents.Number_of_Casualties) as TOTAL_CASUALTIES;

ORDER_DATA = ORDER GROUP_SUM by TOTAL_CASUALTIES DESC;

STORE ORDER_DATA INTO 'Total_Casualty_Report';
