--EXEC JOBS_FAILS  

/*
ESTA PROCEDURE TEM COMO OBJETIVO PUXAR O STATUS DA ULTIMA EXECUCAO DE CADA JOB E FILTRAR SOMENTE OS JOBS COM ERROS E OU ALERTAS
*/

CREATE OR ALTER PROC JOBS_FAILS 
AS
	BEGIN


		/*
		VERIFICACAO IMPORATANTE PARA NAO FICAR JOB SEM HISTORICO E GERAR VARIOS STATUS DE UNKNOW
		VERIFICA SE O MAXIMUM JOB HISTORY LOOG SIZE ESTA DIFERENTE DE 100000 E FAZ O AJUSTE
		VERIFICA SE O MAXIMUM JOB HISTORY PER JOB ESTA DIFERENTE DE 100 E FAZ O AJUSTE
		*/

		DECLARE @jobhistory_max_rows         INT
		DECLARE @jobhistory_max_rows_per_job INT  


		--CAPTURA JobHistoryMaxRows
		EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',  
			N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent',  
			N'JobHistoryMaxRows',  
            @jobhistory_max_rows OUTPUT,
            N'no_output'  
		

		--CAPTURA JobHistoryMaxRowsPerJob
		EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',  
            N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent',  
            N'JobHistoryMaxRowsPerJob',  
            @jobhistory_max_rows_per_job OUTPUT
		

		--LOGICA PARA VERIFICAR NECESSIDADE DE ALTERAR jobhistory_max_rows
		IF @jobhistory_max_rows <> 100000 
			BEGIN
				EXEC msdb.dbo.sp_set_sqlagent_properties @jobhistory_max_rows=100000
			END
		

		--LOGICA PARA VERIFICAR NECESSIDADE DE ALTERAR jobhistory_max_rows_per_job
		IF @jobhistory_max_rows_per_job <> 100
			BEGIN
				EXEC msdb.dbo.sp_set_sqlagent_properties @jobhistory_max_rows_per_job=100
			END


		SET ANSI_NULLS OFF  
		SET ANSI_WARNINGS OFF  
		SET NOCOUNT ON  


		-- APAGAR TABELA TEMPORARIA CASO JA EXISTA  
		IF( Object_id('tempdb..#tmp_jobhistory') IS NOT NULL )  
			BEGIN  
				DROP TABLE #tmp_jobhistory  
		END    
  
		--CRIACAO DE TABELA TEMPORARIA  
		CREATE TABLE #tmp_jobhistory(   
			"command" VARCHAR(1000),
			"Qnt_agendamento" int,  
			"enabled" INT,   
			"job_id_SJ" VARCHAR(1000),                                 
			"name_SJ"  VARCHAR(1000),                                 
			"start_step_id_SJ" INT,                                 
			"QNT_Schedule_SUBQUERY" INT,                                 
			"QNT_Step_SUBQUERY" INT,                                 
			"job_history_id_SJA" VARCHAR(1000),  
			"start_execution_date_SJA" DATETIME,  
			"last_executed_step_date_SJA" DATETIME,  
			"stop_execution_date_SJA" DATETIME,  
			"last_executed_step_id_SJA" INT,  
			"next_scheduled_run_date_SJA" DATETIME,  
			"OK" INT,   
			"NOK" INT,  
			"Count_history" INT,  
			"last_run_outcome_ended_SOS" VARCHAR(1000),  
			"last_outcome_message_ended_SOS" VARCHAR(1000),  
			"last_run_ended_SOS" DATETIME,
			"Running" char(1)
			)     

  
			--**BLOCO 1: INSERÇÃO DE DADOS NA TABELA TEMPORARIA JOBS QUE NAO ESTAO EM EXECUCAO** 
			--INSERT
			INSERT INTO #tmp_jobhistory                                 
  
			--SELECT JOBS QUE NAO ESTAO EM EXECUCAO  
			SELECT   
			--REMOVE DUPLICADAS  
			DISTINCT  
			'use MSDB; EXEC sp_start_job  @job_id = '+ ''''+ CAST(SJ.job_id AS VARCHAR(255)) + '''' as "command" ,

			--SUBQUERY  
			(SELECT COUNT (job_id) FROM [MSDB].[dbo].[sysjobschedules] SC WITH(NOLOCK) WHERE SC.JOB_ID = SJ.JOB_ID) AS "Qnt_agendamento",  
  
			SJ.enabled,  
			SJ.job_id as "job_id_SJ",  
			SJ.name as "name_SJ" ,  
			SJ.start_step_id as "start_step_id_SJ",  
  
			--SUBQUERY  
			(SELECT   
			count (sc.job_id)  
			FROM   
			[MSDB].[dbo].[sysjobschedules] SC WITH(NOLOCK)  
			where sc.job_id = SJ.job_id) AS "QNT_Schedule_SUBQUERY",  
  
			--SUBQUERY  
			(SELECT   
			count (st.job_id)  
			FROM   
			[MSDB].[dbo].[sysjobsteps] st WITH(NOLOCK)  
			where SJ.job_id = st.job_id) AS "QNT_Step_SUBQUERY",  
  
			SJA.job_history_id AS "job_history_id_SJA",  
			SJA.start_execution_date AS "start_execution_date_SJA",  
			SJA.last_executed_step_date AS "last_executed_step_date_SJA",  
			SJA.stop_execution_date AS "stop_execution_date_SJA",  
			SJA.last_executed_step_id AS "last_executed_step_id_SJA",  
			SJA.next_scheduled_run_date AS "next_scheduled_run_date_SJA",  
  
			--SUBQUERY  
			(SELECT COUNT(SJ2.run_status) FROM [MSDB].[dbo].[sysjobhistory] SJ2 WITH(NOLOCK) WHERE SJ2.job_id = SJ.job_id   
			AND   
			(convert(datetime, stuff(stuff((CONVERT(VARCHAR(8),SJ2.run_date,112)   
			+ ' ' + replace((REPLACE(STR((convert(VARCHAR(6), SJ2.run_time,108)), 6),' ','0')),':','')) , 12, 0,  ':'), 15, 0, ':')))  
			>=  SJA.start_execution_date   
			AND SJ2.run_status=1   
			) AS OK,  
  
			--SUBQUERY 
			(SELECT COUNT(SJ2.run_status) FROM [MSDB].[dbo].[sysjobhistory] SJ2 WITH(NOLOCK) WHERE SJ2.job_id = SJ.job_id   
			AND   
			(convert(datetime, stuff(stuff((CONVERT(VARCHAR(8),SJ2.run_date,112)   
			+ ' ' + replace((REPLACE(STR((convert(VARCHAR(6), SJ2.run_time,108)), 6),' ','0')),':','')) , 12, 0,  ':'), 15, 0, ':')))  
			>=  SJA.start_execution_date   
			AND SJ2.run_status=0   
			) AS NOK,  
  
			--SUBQUERY CONTAGEM HISTORICO PARA SABER SE TEM HISTORICO  
			(SELECT COUNT(SJ2.run_status) FROM [MSDB].[dbo].[sysjobhistory] SJ2 WITH(NOLOCK) WHERE SJ2.job_id = SJ.job_id ) AS "Count_history",  
  
			--PEGA STATUS ULTIMO JOB, OBS: SE ESTIVER JOB EM EXECUCAO ELE PEGA O PENULTIMO  
			"last_run_outcome_ended_SOS" =   
			CASE   
			WHEN SOS.last_run_outcome = 0  THEN 'Failed'  
			WHEN SOS.last_run_outcome = 1  THEN 'Succeeded'  
			WHEN SOS.last_run_outcome = 2  THEN 'Retries'  
			WHEN SOS.last_run_outcome = 3  THEN 'Canceled'  
			WHEN SOS.last_run_outcome = 4  THEN 'in progress'  
			ELSE 'Unknown' END,  
			SOS.last_outcome_message as "last_outcome_message_ended_SOS",  
			(convert(datetime, stuff(stuff((CONVERT(VARCHAR(8),sos.last_run_date,112)   
			+ ' ' + replace((REPLACE(STR((convert(VARCHAR(6), sos.last_run_time,108)), 6),' ','0')),':','')) , 12, 0,  ':'), 15, 0, ':'))) AS "Last_run_ended_SOS", 'N' 
  
			from [MSDB].[dbo].[sysjobs] SJ WITH(NOLOCK)  
			LEFT JOIN [MSDB].[dbo].[sysjobactivity] SJA WITH(NOLOCK) ON (SJ.job_id = SJA.job_id AND SJA.start_execution_date IS NOT NULL)  
			LEFT JOIN [MSDB].[dbo].[sysjobservers]  SOS WITH(NOLOCK) ON (SJ.job_id = SOS.job_id)  
  
			--SELECT MAX
			WHERE   
			(  
			((SJA.start_execution_date = (SELECT MAX ( SJA_2.start_execution_date )  
			FROM [MSDB].[dbo].[sysjobactivity] SJA_2 WITH(NOLOCK)  
			WHERE SJ.job_id = SJA_2.job_id)  
			)) OR SJA.start_execution_date IS NULL)  
  
			--NAO MOSTRAR JOB EM EXECUCAO  
			AND SJ.job_id NOT IN   
			(  
			SELECT  
				A.job_id  
			FROM  
				msdb.dbo.sysjobactivity                 A   WITH(NOLOCK)  
				LEFT JOIN msdb.dbo.sysjobhistory        B   WITH(NOLOCK)    ON  A.job_history_id = B.instance_id  
				JOIN msdb.dbo.sysjobs                   C   WITH(NOLOCK)    ON  A.job_id = C.job_id  
				JOIN msdb.dbo.sysjobsteps               D   WITH(NOLOCK)    ON  A.job_id = D.job_id AND ISNULL(A.last_executed_step_id, 0) + 1 = D.step_id  
				JOIN msdb.dbo.syscategories             E   WITH(NOLOCK)    ON  C.category_id = E.category_id  
			WHERE  
				A.session_id = ( SELECT TOP 1 session_id FROM msdb.dbo.syssessions WITH(NOLOCK) ORDER BY agent_start_date DESC )   
				AND A.start_execution_date IS NOT NULL   
				AND A.stop_execution_date IS NULL  
			)  


			--**BLOCO 2: INSERÇÃO DE DADOS NA TABELA TEMPORARIA JOBS QUE ESTAO EM EXECUCAO**  
			--INSERT
			INSERT INTO #tmp_jobhistory   
			--SELECT JOBS QUE ESTAO EM EXECUCAO    
			select 
			--REMOVE DUPLICADAS 
			DISTINCT  

			'use MSDB; EXEC sp_start_job  @job_id = '+ ''''+ CAST(SJ4.job_id AS VARCHAR(255)) + '''' as "command" ,

			--SUBQUERY  
			(SELECT COUNT (job_id) FROM [MSDB].[dbo].[sysjobschedules] SC WITH(NOLOCK) WHERE SC.JOB_ID = SJ4.JOB_ID) AS "Qnt_agendamento",  
  
			SJ4.enabled,  
			SJ4.job_id as "job_id_SJ",  
			SJ4.name as "name_SJ" ,  
			SJ4.start_step_id as "start_step_id_SJ",  
  
			--SUBQUERY  
			(SELECT   
			count (sc.job_id)  
			FROM   
			[MSDB].[dbo].[sysjobschedules] SC WITH(NOLOCK)  
			where sc.job_id = SJ4.job_id) AS "QNT_Schedule_SUBQUERY",  
  
			--SUBQUERY  
			(SELECT   
			count (st.job_id)  
			FROM   
			[MSDB].[dbo].[sysjobsteps] st WITH(NOLOCK)  
			where SJ4.job_id = st.job_id) AS "QNT_Step_SUBQUERY",  
  
			null AS "job_history_id_SJA",  
			null AS "start_execution_date_SJA",  
			null AS "last_executed_step_date_SJA",  
			null AS "stop_execution_date_SJA",  
			null AS "last_executed_step_id_SJA",  
			null AS "next_scheduled_run_date_SJA",  
  
			--SUBQUERY 
			(SELECT COUNT(SJ2.run_status) FROM [MSDB].[dbo].[sysjobhistory] SJ2 WITH(NOLOCK) WHERE SJ2.job_id = SJ4.job_id   
			AND   
			(convert(datetime, stuff(stuff((CONVERT(VARCHAR(8),SJ2.run_date,112)   
			+ ' ' + replace((REPLACE(STR((convert(VARCHAR(6), SJ2.run_time,108)), 6),' ','0')),':','')) , 12, 0,  ':'), 15, 0, ':')))  
			>=    
			(convert(datetime, stuff(stuff((CONVERT(VARCHAR(8),SOS4.last_run_date,112)   
			+ ' ' + replace((REPLACE(STR((convert(VARCHAR(6), SOS4.last_run_time,108)), 6),' ','0')),':','')) , 12, 0,  ':'), 15, 0, ':')))  
			AND SJ2.run_status=1   
			) AS OK,  
  
			--SUBQUERY  
			(SELECT COUNT(SJ2.run_status) FROM [MSDB].[dbo].[sysjobhistory] SJ2 WITH(NOLOCK) WHERE SJ2.job_id = SJ4.job_id   
			AND   
			(convert(datetime, stuff(stuff((CONVERT(VARCHAR(8),SJ2.run_date,112)   
			+ ' ' + replace((REPLACE(STR((convert(VARCHAR(6), SJ2.run_time,108)), 6),' ','0')),':','')) , 12, 0,  ':'), 15, 0, ':')))  
			>=    
			(convert(datetime, stuff(stuff((CONVERT(VARCHAR(8),SOS4.last_run_date,112)   
			+ ' ' + replace((REPLACE(STR((convert(VARCHAR(6), SOS4.last_run_time,108)), 6),' ','0')),':','')) , 12, 0,  ':'), 15, 0, ':')))  
			AND SJ2.run_status=0   
			) AS NOK,  
  
			--SUBQUERY CONTAGEM HISTORICO PARA SABER SE TEM HISTORICO  
			(SELECT COUNT(SJ2.run_status) FROM [MSDB].[dbo].[sysjobhistory] SJ2 WITH(NOLOCK) WHERE SJ2.job_id = SJ4.job_id ) AS "Count_history",  
  
			--PEGA STATUS ULTIMO JOB, OBS: SE ESTIVER JOB EM EXECUCAO ELE PEGA O PENULTIMO  
			"last_run_outcome_ended_SOS" =   
			CASE   
			WHEN SOS4.last_run_outcome = 0  THEN 'Failed'  
			WHEN SOS4.last_run_outcome = 1  THEN 'Succeeded'  
			WHEN SOS4.last_run_outcome = 2  THEN 'Retries'  
			WHEN SOS4.last_run_outcome = 3  THEN 'Canceled'  
			WHEN SOS4.last_run_outcome = 4  THEN 'in progress'  
			ELSE 'Unknown' END,  
			SOS4.last_outcome_message as "last_outcome_message_ended_SOS",  
			(convert(datetime, stuff(stuff((CONVERT(VARCHAR(8),SOS4.last_run_date,112)   
			+ ' ' + replace((REPLACE(STR((convert(VARCHAR(6), SOS4.last_run_time,108)), 6),' ','0')),':','')) , 12, 0,  ':'), 15, 0, ':'))) AS "Last_run_ended_SOS"  , 'S'
  
			from [MSDB].[dbo].[sysjobs] SJ4 WITH(NOLOCK)  
			LEFT JOIN [MSDB].[dbo].[sysjobservers] SOS4 ON (SOS4.job_id = SJ4.job_id)  
			WHERE SJ4.job_id NOT IN   
			(  
			--VERSAO COMPARACAO TABELA TEMPORARIA
			SELECT   
			T2.job_id_SJ  
			FROM #tmp_jobhistory T2  
			)  


			--BLOCO 3: SELECT TABELA TEMPORARIA, SETANDO REGRAS DE NEGOCIO
			--CASE WHEEN, STATUS REMOVIDOS, REMOCAO FALHAS ANTIGAS
			SELECT DISTINCT * FROM   
			(  
			SELECT TJOBS.name_SJ as "Nome do Job",  
			CASE WHEN TJOBS.start_execution_date_SJA IS NOT NULL THEN TJOBS.start_execution_date_SJA ELSE TJOBS.last_run_ended_SOS end as "Ultima Execução",
			case when TJOBS.next_scheduled_run_date_SJA is null and TJOBS.Running = 'S' then 'Job Running'
			else convert(varchar, TJOBS.next_scheduled_run_date_SJA, 25) end as "Proxima Execução",
			"Status da Ultima Execução" = CASE  
			WHEN TJOBS.enabled = 0 THEN 'Job Disable'  
			WHEN TJOBS.Qnt_agendamento = 0 THEN 'Job Without Schedule'  
			WHEN Count_history = 0 THEN 'Job Without History' + ' Last Run: ' +TJOBS.last_run_outcome_ended_SOS  
			WHEN TJOBS.last_run_outcome_ended_SOS = 'Canceled' then 'Job Canceled'  
			WHEN TJOBS.last_run_outcome_ended_SOS = 'Failed'  then 'Job Failed'  
			WHEN TJOBS.OK >0 AND TJOBS.NOK = 0 THEN 'Job Succeeded'  
			WHEN TJOBS.OK >0 AND TJOBS.NOK > 0 THEN 'Job Alert'  
			WHEN TJOBS.last_run_outcome_ended_SOS = 'Unknown' THEN 'Unknown SOS'  
			WHEN TJOBS.job_history_id_SJA IS NULL THEN 'Job Without History' + ' Last Run: ' +TJOBS.last_run_outcome_ended_SOS  
			ELSE 'Error'
			END,
			TJOBS.command as "Comando Executavel"
			FROM #tmp_jobhistory  TJOBS WITH(NOLOCK)) AS JOBS 
			--REMOVER STATUS QUE DEVEM SER DESCONSIDERADOS
			WHERE JOBS.[Status da Ultima Execução] NOT IN ('Job Disable', 'Job Succeeded','Job Without Schedule','Job Without History Last Run: Succeeded','Job Without History Last Run: Unknown')  
			--REMOVER JOBS ANTERIOR A 48 HORAS
			AND JOBS.[Ultima Execução] >= (GETDATE()-2)
			order by 1 desc  
  

			--APAGAR TABELA TEMPORARIA  
			DROP TABLE #tmp_jobhistory  
  

			SET ANSI_NULLS ON  
			SET ANSI_WARNINGS ON  
			SET NOCOUNT OFF  


	END