truncate table rm_prod2_db.inactivated_paths;
truncate table rm_prod2_db.v_inactivated_paths;

BEGIN TRANSACTION;

DELETE      
    FROM
        RM_PROD2_DB.path_leg         
    WHERE
        EXISTS  (
            SELECT
                *               
            FROM
                RM_PROD2_DB.path_leg    AS a                
            WHERE
                a.path_id = RM_PROD2_DB.PATH_LEG.path_id                   
                AND a.SCH_LEG_ACTIVE_DT = RM_PROD2_DB.PATH_LEG.SCH_LEG_ACTIVE_DT                    
                AND a.FLT_ID = RM_PROD2_DB.PATH_LEG.FLT_ID                    
                AND a.SCH_LEG_EFF_DT = RM_PROD2_DB.PATH_LEG.SCH_LEG_EFF_DT                    
                AND a.SCH_LEG_DISC_DT = RM_PROD2_DB.PATH_LEG.SCH_LEG_DISC_DT                    
                AND a.SCH_LEG_ORIG_CD = RM_PROD2_DB.PATH_LEG.SCH_LEG_ORIG_CD                    
                AND a.SCH_LEG_DEST_CD = RM_PROD2_DB.PATH_LEG.SCH_LEG_DEST_CD                    
                AND a.LEG_FREQ_BITMASK = RM_PROD2_DB.PATH_LEG.LEG_FREQ_BITMASK                    
                AND RM_PROD2_DB.PATH_LEG.sch_leg_inactive_dt > a.sch_leg_inactive_dt             
        );

UPDATE rm_prod2_db.PATH_LEG pl
SET SCH_LEG_INACTIVE_DT = fsl.maxina
from (
			   select
				max(case
				  when sch_leg_active_dt = dateadd(day,1,':FEEDDATE') then cast('3001-01-01' as date)
				  else SCH_LEG_INACTIVE_DT
				 end) as maxina,
				SCH_LEG_FREQ_BITMASK,  -- LEG_FREQ_BITMASK,  when from rm_prod_db
				SCH_LEG_DISC_DT,
				SCH_LEG_ORIG_CD,
				FLT_ID,
				SCH_LEG_ACTIVE_DT,
				SCH_LEG_DEST_CD,
				SCH_LEG_EFF_DT,
				count(*) as tot
			   from co_prod_vmdb.flt_sch_leg
			   where sch_leg_active_dt <= dateadd(day,1,':FEEDDATE')
			   group by
				SCH_LEG_FREQ_BITMASK,
				SCH_LEG_DISC_DT,
				SCH_LEG_ORIG_CD,
				FLT_ID,
				SCH_LEG_ACTIVE_DT,
				SCH_LEG_DEST_CD,
				SCH_LEG_EFF_DT
			) fsl
       WHERE
    		fsl.maxina < pl.SCH_LEG_INACTIVE_DT
		   AND pl.SCH_LEG_INACTIVE_DT = '3001-01-01'
		   AND fsl.SCH_LEG_ACTIVE_DT = pl.SCH_LEG_ACTIVE_DT
		   AND fsl.FLT_ID = pl.FLT_ID
		   AND fsl.SCH_LEG_EFF_DT = pl.SCH_LEG_EFF_DT
		   AND fsl.SCH_LEG_DISC_DT = pl.SCH_LEG_DISC_DT
		   AND fsl.SCH_LEG_ORIG_CD = pl.SCH_LEG_ORIG_CD
		   AND fsl.SCH_LEG_DEST_CD = pl.SCH_LEG_DEST_CD
		   AND fsl.SCH_LEG_FREQ_BITMASK = pl.LEG_FREQ_BITMASK;

insert into rm_prod2_db.v_inactivated_paths
select pl.path_id,pl.minina from (select path_id,
		min(sch_leg_inactive_dt) minina
	from rm_prod2_db.path_leg 
	group by path_id) pl, rm_prod2_db.path p where p.path_id = pl.path_id 
	and p.PATH_INACTIVE_DT = '3001-01-01' and minina < p.path_inactive_dt;
	
UPDATE rm_prod2_db.PATH
SET PATH_INACTIVE_DT = ip.minina
from rm_prod2_db.v_inactivated_paths ip
WHERE path.path_id = ip.path_id
     AND PATH.PATH_INACTIVE_DT = '3001-01-01'
    and  ip.minina < path.path_inactive_dt
;

insert into rm_prod2_db.inactivated_paths
select i.path_id,p.path_dscr from rm_prod2_db.v_inactivated_paths i
join rm_prod2_db.path p
on p.path_id = i.path_id;

END TRANSACTION;