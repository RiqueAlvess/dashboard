from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import pandas as pd
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Gerenciador de operações de banco de dados para o sistema"""
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_session(self) -> Session:
        """Retorna uma nova sessão de banco de dados"""
        return self.SessionLocal()
    
    def create_all_tables(self):
        """Cria todas as tabelas no banco de dados"""
        from sqlalchemy_models import Base
        Base.metadata.create_all(bind=self.engine)
        logger.info("Todas as tabelas foram criadas com sucesso")
    
    def truncate_all_tables(self):
        """Limpa todas as tabelas seguindo a regra de negócio"""
        with self.get_session() as session:
            try:
                # Ordem correta para evitar problemas de FK
                tables_order = [
                    'fato_absenteismo',
                    'fato_convocacao', 
                    'fato_cat',
                    'fato_vencimento',
                    'dim_funcionario',
                    'dim_exame',
                    'dim_tempo',
                    'dim_setor',
                    'dim_cargo',
                    'dim_unidade',
                    'dim_empresa'
                ]
                
                # Desabilita constraints temporariamente
                session.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
                
                for table in tables_order:
                    session.execute(text(f"TRUNCATE TABLE {table};"))
                    logger.info(f"Tabela {table} truncada")
                
                # Reabilita constraints
                session.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
                session.commit()
                logger.info("Todas as tabelas foram limpas com sucesso")
                
            except Exception as e:
                session.rollback()
                logger.error(f"Erro ao truncar tabelas: {e}")
                raise


class DataLoader:
    """Classe para carregar dados nas tabelas seguindo as regras de negócio"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def load_dim_tempo(self, start_date: date, end_date: date):
        """Carrega a dimensão tempo para o período especificado"""
        from sqlalchemy_models import DimTempo
        
        with self.db_manager.get_session() as session:
            try:
                current_date = start_date
                tempo_records = []
                
                while current_date <= end_date:
                    # Calcula atributos da data
                    weekday = current_date.weekday() + 1  # 1=Segunda, 7=Domingo
                    is_weekend = weekday in [6, 7]
                    
                    tempo_record = DimTempo(
                        data_completa=current_date,
                        ano=current_date.year,
                        trimestre=(current_date.month - 1) // 3 + 1,
                        mes=current_date.month,
                        dia=current_date.day,
                        dia_semana=weekday,
                        semana_ano=current_date.isocalendar()[1],
                        nome_mes=current_date.strftime('%B'),
                        nome_dia_semana=current_date.strftime('%A'),
                        eh_final_semana=is_weekend,
                        eh_feriado=False  # Pode ser customizado
                    )
                    tempo_records.append(tempo_record)
                    
                    # Incrementa um dia
                    current_date = current_date.replace(day=current_date.day + 1) \
                        if current_date.day < 28 else \
                        date(current_date.year + (current_date.month // 12), 
                             (current_date.month % 12) + 1, 1)
                
                session.add_all(tempo_records)
                session.commit()
                logger.info(f"Carregados {len(tempo_records)} registros na dim_tempo")
                
            except Exception as e:
                session.rollback()
                logger.error(f"Erro ao carregar dim_tempo: {e}")
                raise
    
    def load_empresas(self, empresas_data: List[Dict[str, Any]]):
        """Carrega dados das empresas"""
        from sqlalchemy_models import DimEmpresa
        
        with self.db_manager.get_session() as session:
            try:
                empresas = []
                for emp_data in empresas_data:
                    empresa = DimEmpresa(
                        codigo_empresa=emp_data['codigo_empresa'],
                        nome_empresa=emp_data['nome_empresa'],
                        cnpj=emp_data.get('cnpj'),
                        situacao=emp_data.get('situacao', 'ATIVA')
                    )
                    empresas.append(empresa)
                
                session.add_all(empresas)
                session.commit()
                logger.info(f"Carregadas {len(empresas)} empresas")
                
            except Exception as e:
                session.rollback()
                logger.error(f"Erro ao carregar empresas: {e}")
                raise
    
    def load_funcionarios(self, funcionarios_data: List[Dict[str, Any]]):
        """Carrega dados dos funcionários com implementação SCD Tipo 2"""
        from sqlalchemy_models import DimFuncionario
        
        with self.db_manager.get_session() as session:
            try:
                funcionarios = []
                for func_data in funcionarios_data:
                    # Converte datas string para date objects se necessário
                    data_nascimento = self._parse_date(func_data.get('DATA_NASCIMENTO'))
                    data_admissao = self._parse_date(func_data.get('DATA_ADMISSAO'))
                    data_demissao = self._parse_date(func_data.get('DATA_DEMISSAO'))
                    data_ultima_alteracao = self._parse_date(func_data.get('DATAULTALTERACAO'))
                    
                    funcionario = DimFuncionario(
                        codigo_funcionario=func_data['CODIGO'],
                        codigo_empresa=func_data['CODIGOEMPRESA'],
                        nome=func_data['NOME'],
                        cpf=func_data['CPF'],
                        rg=func_data.get('RG'),
                        uf_rg=func_data.get('UFRG'),
                        orgao_emissor_rg=func_data.get('ORGAOEMISSORRG'),
                        data_nascimento=data_nascimento,
                        sexo=func_data['SEXO'],
                        estado_civil=func_data.get('ESTADOCIVIL'),
                        matricula_funcionario=func_data['MATRICULAFUNCIONARIO'],
                        situacao=func_data['SITUACAO'],
                        data_admissao=data_admissao,
                        data_demissao=data_demissao,
                        codigo_unidade=func_data.get('CODIGOUNIDADE'),
                        nome_unidade=func_data.get('NOMEUNIDADE'),
                        codigo_setor=func_data.get('CODIGOSETOR'),
                        nome_setor=func_data.get('NOMESETOR'),
                        codigo_cargo=func_data.get('CODIGOCARGO'),
                        nome_cargo=func_data.get('NOMECARGO'),
                        cbo_cargo=func_data.get('CBOCARGO'),
                        cc_custo=func_data.get('CCUSTO'),
                        nome_centro_custo=func_data.get('NOMECENTROCUSTO'),
                        endereco=func_data.get('ENDERECO'),
                        numero_endereco=func_data.get('NUMERO_ENDERECO'),
                        bairro=func_data.get('BAIRRO'),
                        cidade=func_data.get('CIDADE'),
                        uf=func_data.get('UF'),
                        cep=func_data.get('CEP'),
                        telefone_residencial=func_data.get('TELEFONERESIDENCIAL'),
                        telefone_celular=func_data.get('TELEFONECELULAR'),
                        email=func_data.get('EMAIL'),
                        deficiente=bool(func_data.get('DEFICIENTE', 0)),
                        deficiencia=func_data.get('DEFICIENCIA'),
                        nome_mae=func_data.get('NM_MAE_FUNCIONARIO'),
                        data_ultima_alteracao=data_ultima_alteracao,
                        matricula_rh=func_data.get('MATRICULARH'),
                        cor=func_data.get('COR'),
                        escolaridade=func_data.get('ESCOLARIDADE'),
                        naturalidade=func_data.get('NATURALIDADE'),
                        ramal=func_data.get('RAMAL'),
                        regime_revezamento=func_data.get('REGIMEREVEZAMENTO'),
                        regime_trabalho=func_data.get('REGIMETRABALHO'),
                        telefone_comercial=func_data.get('TELCOMERCIAL'),
                        turno_trabalho=func_data.get('TURNOTRABALHO'),
                        rh_unidade=func_data.get('RHUNIDADE'),
                        rh_setor=func_data.get('RHSETOR'),
                        rh_cargo=func_data.get('RHCARGO'),
                        rh_centro_custo_unidade=func_data.get('RHCENTROCUSTOUNIDADE'),
                        pis=func_data.get('PIS'),
                        ctps=func_data.get('CTPS'),
                        serie_ctps=func_data.get('SERIECTPS'),
                        tipo_contratacao=func_data.get('TIPOCONTATACAO')
                    )
                    funcionarios.append(funcionario)
                
                session.add_all(funcionarios)
                session.commit()
                logger.info(f"Carregados {len(funcionarios)} funcionários")
                
            except Exception as e:
                session.rollback()
                logger.error(f"Erro ao carregar funcionários: {e}")
                raise
    
    def load_absenteismo(self, absenteismo_data: List[Dict[str, Any]]):
        """Carrega dados de absenteísmo"""
        from sqlalchemy_models import FatoAbsenteismo, DimFuncionario, DimTempo
        
        with self.db_manager.get_session() as session:
            try:
                absenteismo_records = []
                
                for abs_data in absenteismo_data:
                    # Busca funcionário pela chave composta
                    funcionario = session.query(DimFuncionario).filter(
                        DimFuncionario.codigo_empresa == abs_data.get('EMPRESA', 0),
                        DimFuncionario.nome_unidade == abs_data['UNIDADE'],
                        DimFuncionario.nome_setor == abs_data['SETOR'],
                        DimFuncionario.data_nascimento == self._parse_date(abs_data['DT_NASCIMENTO']),
                        DimFuncionario.sexo == abs_data['SEXO'],
                        DimFuncionario.registro_ativo == True
                    ).first()
                    
                    if not funcionario:
                        logger.warning(f"Funcionário não encontrado para absenteísmo: {abs_data}")
                        continue
                    
                    # Busca dimensões tempo
                    data_inicio = self._parse_date(abs_data['DT_INICIO_ATESTADO'])
                    data_fim = self._parse_date(abs_data.get('DT_FIM_ATESTADO'))
                    
                    tempo_inicio = session.query(DimTempo).filter(
                        DimTempo.data_completa == data_inicio
                    ).first()
                    
                    tempo_fim = None
                    if data_fim:
                        tempo_fim = session.query(DimTempo).filter(
                            DimTempo.data_completa == data_fim
                        ).first()
                    
                    absenteismo = FatoAbsenteismo(
                        sk_funcionario=funcionario.sk_funcionario,
                        sk_tempo_inicio=tempo_inicio.sk_tempo if tempo_inicio else None,
                        sk_tempo_fim=tempo_fim.sk_tempo if tempo_fim else None,
                        codigo_empresa=abs_data.get('EMPRESA', 0),
                        unidade=abs_data['UNIDADE'],
                        setor=abs_data['SETOR'],
                        data_nascimento=self._parse_date(abs_data['DT_NASCIMENTO']),
                        sexo=abs_data['SEXO'],
                        matricula_funcionario=abs_data['MATRICULA_FUNC'],
                        tipo_atestado=abs_data['TIPO_ATESTADO'],
                        data_inicio_atestado=data_inicio,
                        data_fim_atestado=data_fim,
                        hora_inicio_atestado=abs_data.get('HORA_INICIO_ATESTADO'),
                        hora_fim_atestado=abs_data.get('HORA_FIM_ATESTADO'),
                        dias_afastados=abs_data.get('DIAS_AFASTADOS', 0),
                        horas_afastado=abs_data.get('HORAS_AFASTADO'),
                        cid_principal=abs_data.get('CID_PRINCIPAL'),
                        descricao_cid=abs_data.get('DESCRICAO_CID'),
                        grupo_patologico=abs_data.get('GRUPO_PATOLOGICO'),
                        tipo_licenca=abs_data.get('TIPO_LICENCA')
                    )
                    absenteismo_records.append(absenteismo)
                
                session.add_all(absenteismo_records)
                session.commit()
                logger.info(f"Carregados {len(absenteismo_records)} registros de absenteísmo")
                
            except Exception as e:
                session.rollback()
                logger.error(f"Erro ao carregar absenteísmo: {e}")
                raise
    
    def _parse_date(self, date_str: Any) -> Optional[date]:
        """Converte string de data para objeto date"""
        if not date_str or date_str == '':
            return None
        
        if isinstance(date_str, date):
            return date_str
            
        if isinstance(date_str, str):
            try:
                # Tenta formatos comuns
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y%m%d']:
                    try:
                        return datetime.strptime(date_str, fmt).date()
                    except ValueError:
                        continue
            except:
                pass
        
        return None


class ReportGenerator:
    """Gerador de relatórios e análises"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def get_absenteismo_por_mes(self, ano: int, codigo_empresa: Optional[int] = None) -> pd.DataFrame:
        """Retorna relatório de absenteísmo por mês"""
        query = """
        SELECT 
            dt.ano,
            dt.mes,
            dt.nome_mes,
            fa.codigo_empresa,
            de.nome_empresa,
            COUNT(*) as total_afastamentos,
            SUM(fa.dias_afastados) as total_dias_afastados,
            AVG(fa.dias_afastados) as media_dias_por_afastamento,
            COUNT(DISTINCT fa.sk_funcionario) as funcionarios_afetados
        FROM fato_absenteismo fa
        JOIN dim_tempo dt ON fa.sk_tempo_inicio = dt.sk_tempo
        JOIN dim_funcionario df ON fa.sk_funcionario = df.sk_funcionario  
        JOIN dim_empresa de ON df.codigo_empresa = de.codigo_empresa
        WHERE dt.ano = :ano
        """
        
        params = {'ano': ano}
        if codigo_empresa:
            query += " AND fa.codigo_empresa = :codigo_empresa"
            params['codigo_empresa'] = codigo_empresa
            
        query += """
        GROUP BY dt.ano, dt.mes, dt.nome_mes, fa.codigo_empresa, de.nome_empresa
        ORDER BY dt.mes
        """
        
        with self.db_manager.get_session() as session:
            result = session.execute(text(query), params)
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def get_vencimentos_proximos(self, dias: int = 30) -> pd.DataFrame:
        """Retorna documentos que vencem nos próximos X dias"""
        query = """
        SELECT 
            fv.codigo_empresa,
            de.nome_empresa,
            fv.nome_unidade,
            fv.nome_produto,
            fv.data_vencimento,
            fv.situacao,
            fv.grau_risco,
            DATEDIFF(fv.data_vencimento, CURDATE()) as dias_para_vencimento
        FROM fato_vencimento fv
        JOIN dim_empresa de ON fv.sk_empresa = de.sk_empresa
        WHERE fv.data_vencimento BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL :dias DAY)
        ORDER BY fv.data_vencimento ASC
        """
        
        with self.db_manager.get_session() as session:
            result = session.execute(text(query), {'dias': dias})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    
    def get_funcionarios_convocacao_pendente(self, codigo_empresa: Optional[int] = None) -> pd.DataFrame:
        """Retorna funcionários com convocações pendentes"""
        query = """
        SELECT 
            fc.codigo_empresa,
            fc.nome_funcionario,
            fc.matricula,
            fc.nome_exame,
            fc.data_ultimo_pedido,
            fc.periodicidade,
            fc.refazer,
            CASE 
                WHEN fc.refazer = 1 THEN 'REFAZER'
                WHEN fc.data_ultimo_pedido IS NULL THEN 'PRIMEIRO EXAME'
                ELSE 'PERIODICO'
            END as tipo_convocacao
        FROM fato_convocacao fc
        WHERE fc.refazer = 1 OR fc.data_resultado IS NULL
        """
        
        params = {}
        if codigo_empresa:
            query += " AND fc.codigo_empresa = :codigo_empresa"
            params['codigo_empresa'] = codigo_empresa
            
        query += " ORDER BY fc.data_ultimo_pedido ASC"
        
        with self.db_manager.get_session() as session:
            result = session.execute(text(query), params)
            return pd.DataFrame(result.fetchall(), columns=result.keys())


# Exemplo de uso
if __name__ == "__main__":
    # Configuração da conexão
    DATABASE_URL = "mysql+pymysql://usuario:senha@localhost/nome_banco"
    
    # Inicialização
    db_manager = DatabaseManager(DATABASE_URL)
    data_loader = DataLoader(db_manager)
    report_generator = ReportGenerator(db_manager)
    
    # Criação das tabelas
    db_manager.create_all_tables()
    
    # Pipeline de carga (seguindo a regra de sempre reescrever)
    try:
        # 1. Limpa todas as tabelas
        db_manager.truncate_all_tables()
        
        # 2. Carrega dimensões primeiro
        data_loader.load_dim_tempo(date(2020, 1, 1), date(2025, 12, 31))
        
        # 3. Carrega dados de empresas (exemplo)
        empresas_exemplo = [
            {'codigo_empresa': 1, 'nome_empresa': 'Empresa A', 'cnpj': '12345678000199'},
            {'codigo_empresa': 2, 'nome_empresa': 'Empresa B', 'cnpj': '98765432000188'}
        ]
        data_loader.load_empresas(empresas_exemplo)
        
        # 4. Carrega funcionários
        # funcionarios_data = carregar_dados_funcionarios()  # Implementar
        # data_loader.load_funcionarios(funcionarios_data)
        
        # 5. Carrega fatos
        # absenteismo_data = carregar_dados_absenteismo()  # Implementar  
        # data_loader.load_absenteismo(absenteismo_data)
        
        logger.info("Pipeline de carga executado com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro no pipeline de carga: {e}")
        raise
