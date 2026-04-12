"""
Sistema de Upload e Processamento de Arquivos
Gerencia upload de arquivos CSV de pedidos e processamento automático via ETL
"""
import streamlit as st
import pandas as pd
import os
import re
import tempfile
import shutil
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from datetime import datetime
import sys

class FileUploadManager:
    """Gerenciador de upload e processamento de arquivos"""
    
    def __init__(self):
        self.upload_dir = Path("dados_cliente/uploads")
        self.processed_dir = Path("dados_cliente/processed")
        self.temp_dir = Path("dados_cliente/temp")
        
        # Criar diretórios se não existirem
        for dir_path in [self.upload_dir, self.processed_dir, self.temp_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def validate_filename(self, filename: str) -> bool:
        """
        Valida se o nome do arquivo segue o padrão esperado
        Padrão: "Exportar Consulta de Pedidos*.csv"
        """
        pattern = r"^Exportar Consulta de Pedidos.*\.csv$"
        return bool(re.match(pattern, filename, re.IGNORECASE))
    
    def extract_date_from_filename(self, filename: str) -> Optional[str]:
        """
        Extrai data do nome do arquivo se presente
        Exemplo: "Exportar Consulta de Pedidos-2025-10-08 13_35_58.csv"
        """
        date_pattern = r"(\d{4}-\d{2}-\d{2})"
        match = re.search(date_pattern, filename)
        return match.group(1) if match else None
    
    def save_uploaded_file(self, uploaded_file) -> Tuple[bool, str, Optional[Path]]:
        """
        Salva arquivo carregado e retorna status
        Returns: (success, message, file_path)
        """
        try:
            # Validar nome do arquivo
            if not self.validate_filename(uploaded_file.name):
                return False, f"❌ Nome do arquivo inválido. Use o padrão: 'Exportar Consulta de Pedidos*.csv'", None
            
            # Gerar nome único para evitar conflitos
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = f"{timestamp}_{uploaded_file.name}"
            file_path = self.upload_dir / safe_filename
            
            # Salvar arquivo
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            return True, f"✅ Arquivo salvo: {uploaded_file.name}", file_path
            
        except Exception as e:
            return False, f"❌ Erro ao salvar arquivo: {str(e)}", None
    
    def validate_csv_structure(self, file_path: Path) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """
        Valida estrutura do CSV usando o pipeline do adaptador de cosméticos
        """
        try:
            # Tentar diferentes encodings (mesmo padrão do adaptador)
            encodings = ['utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1', 'utf-8']
            df = None
            used_encoding = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, sep=';', encoding=encoding, nrows=10)  # Preview apenas
                    used_encoding = encoding
                    break
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
            
            if df is None:
                return False, "❌ Não foi possível ler o arquivo com nenhum encoding", None
            
            # Usar a mesma validação do adaptador de cosméticos
            from dados_cliente.adaptador_cosmeticos import normalize_column_names, apply_cosmeticos_aliases
            
            # Normalizar e aplicar aliases (mesmo processo do pipeline)
            df_normalized = normalize_column_names(df.copy())
            df_mapped = apply_cosmeticos_aliases(df_normalized)
            
            # Verificar se temos colunas essenciais após o mapeamento
            essential_columns = ['order_id', 'order_purchase_timestamp', 'price']
            mapped_columns = [col for col in essential_columns if col in df_mapped.columns]
            
            if len(mapped_columns) < 2:  # Pelo menos 2 das 3 colunas essenciais
                available_cols = list(df.columns)[:10]  # Mostrar primeiras 10 colunas
                return False, f"❌ Estrutura não reconhecida. Colunas encontradas: {', '.join(available_cols)}...", df
            
            return True, f"✅ Arquivo compatível - {len(df.columns)} colunas, encoding: {used_encoding}", df
            
        except Exception as e:
            return False, f"❌ Erro ao validar arquivo: {str(e)}", None
    
    def process_file_with_etl(self, file_path: Path) -> Tuple[bool, str, Optional[Path]]:
        """
        Processa arquivo através do pipeline ETL simplificado
        Returns: (success, message, processed_file_path)
        """
        try:
            # Preparar diretório de saída
            output_name = f"processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            output_dir = self.processed_dir / output_name
            output_dir.mkdir(exist_ok=True)
            
            # Usar processador ETL simplificado
            from utils.simple_etl_processor import process_uploaded_file
            
            success, message, processed_file_path = process_uploaded_file(
                str(file_path), 
                str(output_dir)
            )
            
            if success and processed_file_path:
                return True, f"✅ {message}", Path(processed_file_path)
            else:
                return False, f"❌ {message}", None
                
        except Exception as e:
            return False, f"❌ Erro no processamento ETL: {str(e)}", None
    
    def cleanup_temp_files(self, keep_processed: bool = True):
        """
        Limpa arquivos temporários
        """
        try:
            # Limpar uploads antigos (>7 dias)
            cutoff_time = datetime.now().timestamp() - (7 * 24 * 3600)
            
            for file_path in self.upload_dir.glob("*"):
                if file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
            
            # Limpar temp
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.temp_dir.mkdir(exist_ok=True)
                
        except Exception as e:
            st.warning(f"Aviso: Erro na limpeza de arquivos temporários: {str(e)}")

def render_file_upload_section() -> Optional[Path]:
    """
    Renderiza seção de upload de arquivo na sidebar
    Returns: Path para arquivo processado se disponível
    """
    upload_manager = FileUploadManager()
    
    st.sidebar.markdown("### 📁 **Fonte de Dados**")
    
    # Verificar se já existe arquivo processado na sessão
    if 'processed_file_path' in st.session_state and st.session_state.processed_file_path:
        processed_path = Path(st.session_state.processed_file_path)
        if processed_path.exists():
            st.sidebar.success("✅ **Dados carregados**")
            
            # Extrair info do arquivo
            file_info = st.session_state.get('file_info', {})
            if file_info:
                st.sidebar.info(f"📄 **Arquivo:** {file_info.get('original_name', 'N/A')}")
                if 'upload_time' in file_info:
                    st.sidebar.info(f"🕒 **Carregado:** {file_info['upload_time']}")
            
            # Botão para carregar novo arquivo
            if st.sidebar.button("🔄 **Carregar Novo Arquivo**", key="reload_file"):
                # Limpar dados da sessão
                for key in ['processed_file_path', 'file_info', 'upload_status', '__last_upload_ok__']:
                    if key in st.session_state:
                        del st.session_state[key]
                
                # Limpar parâmetros da URL relacionados ao upload
                try:
                    params_to_remove = ['data', 'parquet']
                    for param in params_to_remove:
                        if param in st.query_params:
                            del st.query_params[param]
                except Exception:
                    pass
                
                st.rerun()
            
            return processed_path
    
    # Interface de upload
    st.sidebar.markdown("**Formato aceito:**")
    st.sidebar.code("Exportar Consulta de Pedidos*.csv")
    
    # Botão para limpar dados atuais e permitir novo upload
    if 'processed_file_path' in st.session_state:
        if st.sidebar.button("🗑️ **Limpar Dados Atuais**", key="clear_current_data"):
            # Limpar dados atuais
            if 'processed_file_path' in st.session_state:
                del st.session_state.processed_file_path
            if 'file_info' in st.session_state:
                del st.session_state.file_info
            if 'upload_status' in st.session_state:
                del st.session_state.upload_status
            # Marcar que houve tentativa de upload para não recarregar PoC
            st.session_state['__upload_attempted__'] = True
            st.rerun()
    
    uploaded_file = st.sidebar.file_uploader(
        "Selecione o arquivo de pedidos:",
        type=['csv'],
        help="Arquivo deve seguir o padrão: 'Exportar Consulta de Pedidos-YYYY-MM-DD HH_MM_SS.csv'"
    )
    
    # Marcar que houve tentativa de upload se arquivo foi selecionado
    if uploaded_file is not None:
        st.session_state['__upload_attempted__'] = True
    
    if uploaded_file is not None:
        # Salvar arquivo
        success, message, file_path = upload_manager.save_uploaded_file(uploaded_file)
        
        if not success:
            st.sidebar.error(message)
            return None
        
        st.sidebar.success(message)
        
        # Validar estrutura
        with st.sidebar:
            with st.spinner("🔍 Validando arquivo..."):
                valid, validation_msg, preview_df = upload_manager.validate_csv_structure(file_path)
        
        if not valid:
            st.sidebar.error(validation_msg)
            return None
        
        st.sidebar.success(validation_msg)
        
        # Mostrar preview
        if preview_df is not None:
            with st.sidebar.expander("👀 **Preview dos Dados**"):
                st.dataframe(preview_df, use_container_width=True)
        
        # Processar arquivo
        if st.sidebar.button("🚀 **Processar Dados**", key="process_file"):
            # st.sidebar.info("🔄 Iniciando processamento...")
            with st.sidebar:
                with st.spinner("⚙️ Processando dados via ETL..."):
                    success, process_msg, processed_path = upload_manager.process_file_with_etl(file_path)
            
            if success and processed_path:
                st.sidebar.success(process_msg)
                # st.sidebar.info(f"📁 Arquivo processado: {processed_path}")
                
                # Salvar informações na sessão
                st.session_state.processed_file_path = str(processed_path)
                st.session_state.file_info = {
                    'original_name': uploaded_file.name,
                    'upload_time': datetime.now().strftime("%d/%m/%Y %H:%M"),
                    'file_date': upload_manager.extract_date_from_filename(uploaded_file.name)
                }
                st.session_state.upload_status = 'completed'
                # Sinalizar que houve upload recente (ajuda a lógica de carregamento)
                st.session_state['__last_upload_ok__'] = True
                
                # Persistir na URL para sobreviver à navegação/refresh
                try:
                    from urllib.parse import quote
                    st.query_params['data'] = 'upload'
                    st.query_params['parquet'] = quote(str(processed_path))
                except Exception:
                    pass
                
                # Limpar arquivos temporários
                upload_manager.cleanup_temp_files()
                
                # Retornar o caminho do arquivo processado
                return str(processed_path)
            else:
                st.sidebar.error(process_msg)
                return None
    
    # Não carregar dados da PoC automaticamente no modo upload
    # O usuário deve fazer upload manual ou selecionar dados da Olist
    
    return None

def get_upload_status() -> Dict[str, Any]:
    """
    Retorna status atual do upload
    """
    return {
        'has_processed_file': 'processed_file_path' in st.session_state,
        'file_info': st.session_state.get('file_info', {}),
        'status': st.session_state.get('upload_status', 'none')
    }

def get_default_poc_data() -> Optional[Path]:
    """
    Retorna o caminho para os dados da PoC por padrão se disponível
    """
    # Priorizar dados mais recentes da PoC
    poc_paths = [
        Path("teste_sku_fix/data/dados_processados.parquet"),
        Path("poc_cosmeticos_final/data/dados_processados.parquet"),
        Path("poc_cosmeticos_final/dados_processados_com_categorias.parquet")
    ]
    
    for path in poc_paths:
        if path.exists():
            return path
    
    return None
