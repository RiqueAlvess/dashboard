import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date, time
import re
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Classe para validação de dados de entrada"""
    
    @staticmethod
    def validate_cpf(cpf: str) -> bool:
        """Valida CPF brasileiro"""
        if not cpf:
            return False
            
        # Remove caracteres não numéricos
        cpf = re.sub(r'\D', '', cpf)
        
        # Verifica se tem 11 dígitos
        if len(cpf) != 11:
            return False
            
        # Verifica se não são todos iguais
        if cpf == cpf[0] * 11:
            return False
            
        # Validação dos dígitos verificadores
        def calculate_digit(cpf_partial):
            total = sum(int(digit) * (len(cpf_partial) + 1 - i) 
                       for i, digit in enumerate(cpf_partial))
            remainder = total % 11
            return 0 if remainder < 2 else 11 - remainder
        
        first_digit = calculate_digit(cpf[:9])
        second_digit = calculate_digit(cpf[:10])
        
        return cpf[-2:] == f"{first_digit}{second_digit}"
    
    @staticmethod
    def validate_cnpj(cnpj: str) -> bool:
        """Valida CNPJ brasileiro"""
        if not cnpj:
            return False
            
        # Remove caracteres não numéricos
        cnpj = re.sub(r'\D', '', cnpj)
        
        # Verifica se tem 14 dígitos
        if len(cnpj) != 14:
            return False
            
        # Algoritmo de validação do CNPJ
        def calculate_digit(cnpj_partial, weights):
            total = sum(int(digit) * weight 
                       for digit, weight in zip(cnpj_partial, weights))
            remainder = total % 11
            return 0 if remainder < 2 else 11 - remainder
        
        weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        weights2 = [6, 7, 8, 9, 2, 3, 4, 5, 6, 7, 8, 9]
        
        first_digit = calculate_digit(cnpj[:12], weights1)
        second_digit = calculate_digit(cnpj[:13], weights2)
        
        return cnpj[-2:] == f"{first_digit}{second_digit}"
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Valida formato de email"""
        if not email:
            return True  # Email pode ser vazio
            
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def validate_date_range(start_date: date, end_date: Optional[date] = None) -> bool:
        """Valida se a data de fim é posterior à data de início"""
        if not end_date:
            return True
        return start_date <= end_date


class DataTransformer:
    """Classe para transformações de dados"""
    
    # Mapeamentos de códigos
    SEXO_MAP = {
        1: 'MASCULINO',
        2: 'FEMININO'
    }
    
    ESTADO_CIVIL_MAP = {
        1: 'SOLTEIRO(A)',
        2: 'CASADO(A)', 
        3: 'SEPARADO(A)',
        4: 'DESQUITADO(A)',
        5: 'VIUVO(A)',
        6: 'OUTROS',
        7: 'DIVORCIADO(A)'
    }
    
    TIPO_ATESTADO_MAP = {
        1: 'MEDICO',
        2: 'ODONTOLOGICO',
        3: 'ACIDENTE_TRABALHO',
        4: 'OUTROS'
    }
    
    @staticmethod
    def clean_cpf(cpf: str) -> str:
        """Limpa e formata CPF"""
        if not cpf:
            return None
        return re.sub(r'\D', '', cpf)
    
    @staticmethod
    def clean_cnpj(cnpj: str) -> str:
        """Limpa e formata CNPJ"""
        if not cnpj:
            return None
        return re.sub(r'\D', '', cnpj)
    
    @staticmethod
    def clean_phone(phone: str) -> str:
        """Limpa e formata telefone"""
        if not phone:
            return None
        return re.sub(r'\D', '', phone)
    
    @staticmethod
    def parse_date(date_value: Any) -> Optional[date]:
        """Converte diferentes formatos de data para date object"""
        if pd.isna(date_value) or date_value == '' or date_value is None:
            return None
            
        if isinstance(date_value, date):
            return date_value
            
        if isinstance(date_value, datetime):
            return date_value.date()
            
        if isinstance(date_value, str):
            # Remove espaços e caracteres especiais
            date_value = date_value.strip()
            
            # Formatos comuns brasileiros
            formats = [
                '%d/%m/%Y',
                '%Y-%m-%d', 
                '%d-%m-%Y',
                '%Y%m%d',
                '%d/%m/%y',
                '%Y/%m/%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_value, fmt).date()
                except ValueError:
                    continue
        
        return None
    
    @staticmethod
    def parse_time(time_value: Any) -> Optional[time]:
        """Converte string de hora para time object"""
        if pd.isna(time_value) or time_value == '' or time_value is None:
            return None
            
        if isinstance(time_value, time):
            return time_value
            
        if isinstance(time_value, str):
            time_value = time_value.strip()
            
            # Formatos comuns de hora
            formats = [
                '%H:%M',
                '%H%M', 
                '%H:%M:%S'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(time_value, fmt).time()
                except ValueError:
                    continue
        
        return None
    
    @staticmethod
    def calculate_age(birth_date: date, reference_date: date = None) -> int:
        """Calcula idade baseada na data de nascimento"""
        if not birth_date:
            return None
            
        if not reference_date:
            reference_date = date.today()
            
        age = reference_date.year - birth_date.year
        
        # Ajusta se ainda não fez aniversário no ano
        if reference_date.month < birth_date.month or \
           (reference_date.month == birth_date.month and reference_date.day < birth_date.day):
            age -= 1
            
        return age
    
    @staticmethod
    def calculate_work_days(start_date: date, end_date: date) -> int:
        """Calcula dias úteis entre duas datas"""
        if not start_date or not end_date:
            return 0
            
        return pd.bdate_range(start_date, end_date).size


class AbsenteismoETL:
    """ETL específico para dados de absenteísmo"""
    
    def __init__(self):
        self.validator = DataValidator()
        self.transformer = DataTransformer()
    
    def process_raw_data(self, raw_data: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """Processa dados brutos de absenteísmo"""
        errors = []
        processed_data = raw_data.copy()
        
        # Padronização de colunas
        column_mapping = {
            'UNIDADE': 'unidade',
            'SETOR': 'setor', 
            'MATRICULA_FUNC': 'matricula_funcionario',
            'DT_NASCIMENTO': 'data_nascimento',
            'SEXO': 'sexo',
            'TIPO_ATESTADO': 'tipo_atestado',
            'DT_INICIO_ATESTADO': 'data_inicio_atestado',
            'DT_FIM_ATESTADO': 'data_fim_atestado',
            'HORA_INICIO_ATESTADO': 'hora_inicio_atestado',
            'HORA_FIM_ATESTADO': 'hora_fim_atestado',
            'DIAS_AFASTADOS': 'dias_afastados',
            'HORAS_AFASTADO': 'horas_afastado',
            'CID_PRINCIPAL': 'cid_principal',
            'DESCRICAO_CID': 'descricao_cid',
            'GRUPO_PATOLOGICO': 'grupo_patologico',
            'TIPO_LICENCA': 'tipo_licenca'
        }
        
        processed_data = processed_data.rename(columns=column_mapping)
        
        # Transformações de data
        for col in ['data_nascimento', 'data_inicio_atestado', 'data_fim_atestado']:
            if col in processed_data.columns:
                processed_data[col] = processed_data[col].apply(self.transformer.parse_date)
        
        # Transformações de hora
        for col in ['hora_inicio_atestado', 'hora_fim_atestado']:
            if col in processed_data.columns:
                processed_data[col] = processed_data[col].apply(self.transformer.parse_time)
        
        # Validações
        for idx, row in processed_data.iterrows():
            # Valida datas
            if not self.validator.validate_date_range(
                row.get('data_inicio_atestado'), 
                row.get('data_fim_atestado')
            ):
                errors.append({
                    'row': idx,
                    'error': 'Data fim anterior à data início',
                    'data': row.to_dict()
                })
            
            # Valida dias afastados
            if row.get('dias_afastados', 0) < 0:
                errors.append({
                    'row': idx,
                    'error': 'Dias afastados negativo',
                    'data': row.to_dict()
                })
        
        # Remove registros com erros críticos
        valid_rows = processed_data.dropna(subset=['data_inicio_atestado', 'sexo', 'tipo_atestado'])
        
        # Calcula métricas derivadas
        valid_rows = self._calculate_derived_metrics(valid_rows)
        
        return valid_rows, errors
    
    def _calculate_derived_metrics(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calcula métricas derivadas"""
        data = data.copy()
        
        # Calcula dias afastados se não informado
        mask_missing_days = data['dias_afastados'].isna()
        if mask_missing_days.any():
            data.loc[mask_missing_days, 'dias_afastados'] = data.loc[mask_missing_days].apply(
                lambda row: self.transformer.calculate_work_days(
                    row['data_inicio_atestado'], 
                    row['data_fim_atestado'] or row['data_inicio_atestado']
                ), axis=1
            )
        
        # Adiciona descritivos
        data['sexo_desc'] = data['sexo'].map(self.transformer.SEXO_MAP)
        data['tipo_atestado_desc'] = data['tipo_atestado'].map(self.transformer.TIPO_ATESTADO_MAP)
        
        # Calcula faixas etárias baseadas na data de nascimento
        data['idade_na_data_inicio'] = data.apply(
            lambda row: self.transformer.calculate_age(
                row['data_nascimento'], 
                row['data_inicio_atestado']
            ), axis=1
        )
        
        # Categoriza duração do afastamento
        data['categoria_afastamento'] = pd.cut(
            data['dias_afastados'],
            bins=[0, 1, 3, 7, 15, 30, float('inf')],
            labels=['1 dia', '2-3 dias', '4-7 dias', '8-15 dias', '16-30 dias', '>30 dias'],
            include_lowest=True
        )
        
        return data


class FuncionarioETL:
    """ETL específico para dados de funcionários"""
    
    def __init__(self):
        self.validator = DataValidator()
        self.transformer = DataTransformer()
    
    def process_raw_data(self, raw_data: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """Processa dados brutos de funcionários"""
        errors = []
        processed_data = raw_data.copy()
        
        # Limpeza de dados pessoais
        processed_data['CPF'] = processed_data['CPF'].apply(self.transformer.clean_cpf)
        processed_data['TELEFONECELULAR'] = processed_data['TELEFONECELULAR'].apply(self.transformer.clean_phone)
        processed_data['TELEFONERESIDENCIAL'] = processed_data['TELEFONERESIDENCIAL'].apply(self.transformer.clean_phone)
        
        # Transformações de data
        date_columns = ['DATA_NASCIMENTO', 'DATA_ADMISSAO', 'DATA_DEMISSAO', 'DATAULTALTERACAO']
        for col in date_columns:
            if col in processed_data.columns:
                processed_data[col] = processed_data[col].apply(self.transformer.parse_date)
        
        # Validações
        for idx, row in processed_data.iterrows():
            # Valida CPF
            if row['CPF'] and not self.validator.validate_cpf(row['CPF']):
                errors.append({
                    'row': idx,
                    'error': 'CPF inválido',
                    'cpf': row['CPF']
                })
            
            # Valida email
            if row.get('EMAIL') and not self.validator.validate_email(row['EMAIL']):
                errors.append({
                    'row': idx,
                    'error': 'Email inválido',
                    'email': row['EMAIL']
                })
            
            # Valida datas
            if not self.validator.validate_date_range(
                row.get('DATA_ADMISSAO'), 
                row.get('DATA_DEMISSAO')
            ):
                errors.append({
                    'row': idx,
                    'error': 'Data demissão anterior à data admissão',
                    'data': {'admissao': row.get('DATA_ADMISSAO'), 'demissao': row.get('DATA_DEMISSAO')}
                })
        
        # Calcula métricas derivadas
        processed_data = self._calculate_derived_metrics(processed_data)
        
        return processed_data, errors
    
    def _calculate_derived_metrics(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calcula métricas derivadas para funcionários"""
        data = data.copy()
        
        # Calcula idade atual
        data['idade_atual'] = data['DATA_NASCIMENTO'].apply(
            lambda birth_date: self.transformer.calculate_age(birth_date) if birth_date else None
        )
        
        # Calcula tempo de empresa (em anos)
        def calculate_tenure(admission_date, termination_date=None):
            if not admission_date:
                return None
            end_date = termination_date or date.today()
            return (end_date - admission_date).days / 365.25
        
        data['tempo_empresa'] = data.apply(
            lambda row: calculate_tenure(row['DATA_ADMISSAO'], row['DATA_DEMISSAO']), 
            axis=1
        )
        
        # Adiciona descritivos
        data['sexo_desc'] = data['SEXO'].map(self.transformer.SEXO_MAP)
        data['estado_civil_desc'] = data['ESTADOCIVIL'].map(self.transformer.ESTADO_CIVIL_MAP)
        
        # Categoriza faixas etárias
        data['faixa_etaria'] = pd.cut(
            data['idade_atual'],
            bins=[0, 25, 35, 45, 55, 65, float('inf')],
            labels=['<25', '25-34', '35-44', '45-54', '55-64', '65+'],
            include_lowest=True
        )
        
        # Status de atividade
        data['status_ativo'] = data['DATA_DEMISSAO'].isna()
        
        return data


class VencimentoETL:
    """ETL específico para vencimentos de documentos"""
    
    def __init__(self):
        self.transformer = DataTransformer()
    
    def process_raw_data(self, raw_data: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """Processa dados de vencimentos"""
        errors = []
        processed_data = raw_data.copy()
        
        # Transformações de data
        date_columns = ['dataVencimento', 'dataRealizacaoUltimoServicoRealizado', 'dataPrevisaoUltimoServicoRealizado']
        for col in date_columns:
            if col in processed_data.columns:
                processed_data[col] = processed_data[col].apply(self.transformer.parse_date)
        
        # Calcula métricas de vencimento
        processed_data = self._calculate_vencimento_metrics(processed_data)
        
        return processed_data, errors
    
    def _calculate_vencimento_metrics(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calcula métricas relacionadas a vencimentos"""
        data = data.copy()
        today = date.today()
        
        # Dias para vencimento
        data['dias_para_vencimento'] = data['dataVencimento'].apply(
            lambda venc_date: (venc_date - today).days if venc_date else None
        )
        
        # Status de criticidade
        data['vencido'] = data['dias_para_vencimento'] < 0
        data['critico'] = (data['dias_para_vencimento'] >= 0) & (data['dias_para_vencimento'] <= 30)
        data['alerta'] = (data['dias_para_vencimento'] > 30) & (data['dias_para_vencimento'] <= 60)
        
        # Categoriza status
        def categorize_status(days):
            if pd.isna(days):
                return 'SEM_DATA'
            elif days < 0:
                return 'VENCIDO'
            elif days <= 15:
                return 'CRITICO'
            elif days <= 30:
                return 'ATENCAO'
            elif days <= 60:
                return 'ALERTA'
            else:
                return 'OK'
        
        data['status_vencimento'] = data['dias_para_vencimento'].apply(categorize_status)
        
        return data


# Função principal de ETL
def run_etl_pipeline(
    funcionarios_df: pd.DataFrame,
    absenteismo_df: pd.DataFrame, 
    vencimentos_df: pd.DataFrame,
    convocacoes_df: pd.DataFrame,
    cat_df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Executa pipeline completo de ETL
    
    Returns:
        Dict com DataFrames processados e relatório de erros
    """
    
    results = {
        'data': {},
        'errors': {},
        'summary': {}
    }
    
    # ETL Funcionários
    func_etl = FuncionarioETL()
    funcionarios_clean, func_errors = func_etl.process_raw_data(funcionarios_df)
    results['data']['funcionarios'] = funcionarios_clean
    results['errors']['funcionarios'] = func_errors
    
    # ETL Absenteísmo
    abs_etl = AbsenteismoETL() 
    absenteismo_clean, abs_errors = abs_etl.process_raw_data(absenteismo_df)
    results['data']['absenteismo'] = absenteismo_clean
    results['errors']['absenteismo'] = abs_errors
    
    # ETL Vencimentos
    venc_etl = VencimentoETL()
    vencimentos_clean, venc_errors = venc_etl.process_raw_data(vencimentos_df)
    results['data']['vencimentos'] = vencimentos_clean
    results['errors']['vencimentos'] = venc_errors
    
    # Summary
    results['summary'] = {
        'funcionarios': {
            'total_records': len(funcionarios_df),
            'clean_records': len(funcionarios_clean),
            'errors': len(func_errors)
        },
        'absenteismo': {
            'total_records': len(absenteismo_df),
            'clean_records': len(absenteismo_clean),
            'errors': len(abs_errors)
        },
        'vencimentos': {
            'total_records': len(vencimentos_df),
            'clean_records': len(vencimentos_clean),
            'errors': len(venc_errors)
        }
    }
    
    logger.info(f"ETL Pipeline concluído. Summary: {results['summary']}")
    
    return results
