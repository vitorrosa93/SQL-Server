--CONTADOR DE LOCK

IF (OBJECT_ID('dbo.LOCK_MONITORING') IS NULL)      
	BEGIN  

		/*
		drop table LOCK_MONITORING
		*/

		--Criacao tabela
		Create table LOCK_MONITORING
		(
		id int  NOT NULL IDENTITY PRIMARY KEY,
		cnt_value int,
		Locks int,
		Locks_sec int,
		data_inicio datetime,
		data_fim datetime,
		parametro char(1)
		)

		--Criação indice
		create nonclustered index IX_LOCK_MONITORING_1 on LOCK_MONITORING (data_inicio,data_fim,parametro) include (locks)
		/*
		drop index IX_LOCK_MONITORING_1 on LOCK_MONITORING
		*/

	end


--Declaração de variaveis
DECLARE 
@PARAMETRO CHAR(1),
@LOCK INT, 
@cnt_value int,
@cnt_value_SQL INT

--Verificação contador atual de lock
SELECT @cnt_value_SQL= cntr_value
FROM sys.dm_os_performance_counters
WHERE cntr_type = '272696576'
and counter_name like '%Lock Waits/sec%'
and instance_name = '_total'

--TESTE
--SELECT * FROM LOCK_MONITORING

--Legenda
--CARGA INICIAL: C **Desconsiderar para analise**
--PRIMEIRO VALOR APOS RESET: P **Desconsiderar para analise**
--DADOS NORMAIS: D **Considerar para analise**


--Verificar necessidade de carga inicial
IF (SELECT COUNT(*) FROM LOCK_MONITORING) = 0
	BEGIN
		--carga inicial
		insert into LOCK_MONITORING (cnt_value,locks,locks_sec,data_inicio,data_fim,parametro)
		SELECT cntr_value, 0, 0,  getdate(),getdate(), 'C'
		FROM sys.dm_os_performance_counters
		WHERE cntr_type = '272696576'
		and counter_name like '%Lock Waits/sec%'
		and instance_name = '_total' 
		SELECT 'Feito carga inicial'
	END

--PUXAR ULTIMA LINHA DA TABELA E ALIMENTAR OS ULTIMOS PARAMETROS: @PARAMETRO, @LOCK, @cnt_value
SELECT @PARAMETRO = PARAMETRO , @LOCK = Locks, @cnt_value=  cnt_value FROM LOCK_MONITORING DL
where DL.id = (select max(id) from LOCK_MONITORING)
--TESTE
--SELECT @PARAMETRO
--SELECT @LOCK
--Select @cnt_value

--VERIFICA SE O CNT_VALUE DO SISTEMA É MAIOR QUE O ULTIMO PARAMETRO DA TABELA, ISSO INDICA QUE HOUVE LOCK E  PRECISA SER SALVO NA TABELA
IF 
(SELECT cntr_value FROM sys.dm_os_performance_counters
WHERE cntr_type = '272696576'
and counter_name like '%Lock Waits/sec%'
and instance_name = '_total') > @cnt_value
	BEGIN
		--teste
		--select 'cntr_value maior que tabela'
		insert into LOCK_MONITORING (cnt_value,locks,locks_sec,data_inicio,data_fim,parametro)
		SELECT @cnt_value_SQL, @cnt_value_SQL-@cnt_value, 0,  getdate(),getdate(), 'D'
	END

--VERIFICA SE O CNT_VALUE DO SISTEMA É MENOR QUE O ULTIMO PARAMETRO DA TABELA, ISSO INDICA QUE FOI REICIADO O SERVIDOR O CNTR RESETOU
IF 
(SELECT cntr_value FROM sys.dm_os_performance_counters
WHERE cntr_type = '272696576'
and counter_name like '%Lock Waits/sec%'
and instance_name = '_total') < @cnt_value
	BEGIN
		--teste
		--select 'cntr_value menor que tabela'
		--carga inicial
		insert into LOCK_MONITORING (cnt_value,locks,locks_sec,data_inicio,data_fim,parametro)
		SELECT cntr_value, 0, 0,  getdate(),getdate(), 'C'
		FROM sys.dm_os_performance_counters
		WHERE cntr_type = '272696576'
		and counter_name like '%Lock Waits/sec%'
		and instance_name = '_total' 
		SELECT 'Feito carga inicial'
	END

--VERIFICA SE O CNT_VALUE DO SISTEMA É IGUAL QUE O ULTIMO PARAMETRO DA TABELA, ISSO INDICA QUE NAO FOI REICIADO O SERVIDOR E NEM HOUVE LOCK
IF 
(SELECT cntr_value FROM sys.dm_os_performance_counters
WHERE cntr_type = '272696576'
and counter_name like '%Lock Waits/sec%'
and instance_name = '_total') = @cnt_value
	BEGIN
		--teste
		--select 'cntr_value é igual que tabela'
		IF @PARAMETRO = 'C'
			BEGIN
				insert into LOCK_MONITORING (cnt_value,locks,locks_sec,data_inicio,data_fim,parametro)
				SELECT @cnt_value, @cnt_value, 0,  getdate(),getdate(), 'P'
				END
				IF @PARAMETRO NOT IN ('C')
				BEGIN
				insert into LOCK_MONITORING (cnt_value,locks,locks_sec,data_inicio,data_fim,parametro)
				SELECT @cnt_value, 0, 0,  getdate(),getdate(), 'D'
			END
	END

--saida
SELECT locks,data_inicio,data_fim FROM LOCK_MONITORING WHERE parametro = 'D'