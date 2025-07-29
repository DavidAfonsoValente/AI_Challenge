import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from config import DATABASE_URL

Base = declarative_base()

class Incentive(Base):
    """Represents the 'incentives' table in the database."""
    __tablename__ = 'incentives'
    incentive_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False, unique=True)
    description = Column(Text)
    ai_description = Column(Text)
    document_urls = Column(Text)
    publication_date = Column(Date)
    start_date = Column(Date)
    end_date = Column(Date)
    total_budget = Column(Float)
    source_link = Column(String)

    matches = relationship("Match", back_populates="incentive", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Incentive(id={self.incentive_id}, title='{self.title}')>"

class Company(Base):
    """Represents the 'companies' table in the database."""
    __tablename__ = 'companies'
    nif_code = Column(String, primary_key=True)
    company_name = Column(String, nullable=False)
    last_available_year = Column(Integer)
    operating_revenue_th_eur = Column(Float)
    ebitda_th_eur = Column(Float)
    pl_before_tax_th_eur = Column(Float)
    latest_number_of_employees = Column(Integer)
    nace_secondary_codes = Column(String)
    nace_secondary_labels = Column(String)
    cae_primary_code = Column(String)
    cae_primary_label = Column(String)
    cae_secondary_codes = Column(String)
    cae_secondary_labels = Column(String)
    native_trade_description = Column(Text)
    english_trade_description = Column(Text)
    import_export = Column(String)
    email_portugal = Column(String)
    website = Column(String)
    telephone = Column(String)
    postal_code = Column(String)
    city = Column(String)
    dm_full_name = Column(String)
    dm_job_title = Column(String)
    brand_name = Column(String)
    subsidiary_name = Column(String)
    subsidiary_direct_percent = Column(Float)
    shareholder_name = Column(String)
    shareholder_direct_percent = Column(Float)

    matches = relationship("Match", back_populates="company", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Company(nif='{self.nif_code}', name='{self.company_name}')>"

class Match(Base):
    """Represents the 'matches' table, linking incentives and companies."""
    __tablename__ = 'matches'
    match_id = Column(Integer, primary_key=True, autoincrement=True)
    incentive_id = Column(Integer, ForeignKey('incentives.incentive_id'), nullable=False)
    company_nif = Column(String, ForeignKey('companies.nif_code'), nullable=False)
    score = Column(Float, nullable=False)

    incentive = relationship("Incentive", back_populates="matches")
    company = relationship("Company", back_populates="matches")

    def __repr__(self):
        return f"<Match(incentive_id={self.incentive_id}, company_nif='{self.company_nif}', score={self.score})>"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Dependency function for FastAPI to get a DB session for a request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_database_and_tables():
    """Creates the database file and all defined tables if they don't exist."""
    Base.metadata.create_all(bind=engine)
    print("Database and tables created successfully.")

def load_companies_from_csv(csv_path: str):
    """
    Reads company data from the provided CSV, cleans it, and loads it into the database.
    Handles all fields specified in the CSV with appropriate transformations.
    """
    db = SessionLocal()
    try:
        df = pd.read_csv(csv_path)

        df = df.rename(columns={
            'Company Name': 'company_name',
            'NIF Code': 'nif_code',
            'Last available year': 'last_available_year',
            'Operating revenue / turnover\nth EUR\nLast avail. yr': 'operating_revenue_th_eur',
            'EBITDA\nth EUR\nLast avail. yr': 'ebitda_th_eur',
            'P/L before tax\nth EUR\nLast avail. yr': 'pl_before_tax_th_eur',
            'Latest number of employees': 'latest_number_of_employees',
            'NACE Rev. 2 Secondary Code(s)': 'nace_secondary_codes',
            'NACE Rev. 2 Secondary Label(s)': 'nace_secondary_labels',
            'CAE Rev.3 Primary Code': 'cae_primary_code',
            'CAE Rev.3 Primary Label': 'cae_primary_label',
            'CAE Rev.3 Secondary Code(s)': 'cae_secondary_codes',
            'CAE Rev.3 Secondary Label(s)': 'cae_secondary_labels',
            'Native trade description': 'native_trade_description',
            'English trade description': 'english_trade_description',
            'Import / Export': 'import_export',
            'email portugal': 'email_portugal',
            'Web site': 'website',
            'Telephone': 'telephone',
            'Postal Code': 'postal_code',
            'DM\nFull name': 'dm_full_name',
            'DM Job title (in English)': 'dm_job_title',
            'Brand Name': 'brand_name',
            'Subsidiary - Name': 'subsidiary_name',
            'Subsidiary - Direct %': 'subsidiary_direct_percent',
            'Shareholder - Name': 'shareholder_name',
            'Shareholder - Direct %': 'shareholder_direct_percent'
        })

        df['nif_code'] = df['nif_code'].astype(str)

        df['city'] = df['dm_full_name'].str.extract(r'([A-Z][a-z]+(?: [A-Z][a-z]+)*)$', expand=False)
        df['city'] = df['city'].fillna('Unknown')

        numeric_cols = [
            'last_available_year', 'operating_revenue_th_eur', 'ebitda_th_eur',
            'pl_before_tax_th_eur', 'latest_number_of_employees',
            'subsidiary_direct_percent', 'shareholder_direct_percent'
        ]
        for col in numeric_cols:
            if col in df.columns:
                print(f"Processing column: {col}")
                df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if col in ['latest_number_of_employees', 'last_available_year']:
                    df[col] = df[col].fillna(0).astype(int)
                else:
                    df[col] = df[col].fillna(np.nan)

        for col in ['nace_secondary_codes', 'cae_secondary_codes']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(str(x).split()) if pd.notna(x) else json.dumps([]))

        for _, row in df.iterrows():
            company_data = row.to_dict()
            model_columns = {c.name for c in Company.__table__.columns}
            filtered_data = {k: v for k, v in company_data.items() if k in model_columns}
            company = Company(**filtered_data)
            db.merge(company)

        db.commit()
        print(f"Successfully loaded and processed {len(df)} companies into the database.")

    except FileNotFoundError:
        print(f"[ERROR] The file was not found at path: {csv_path}")
        raise
    except Exception as e:
        db.rollback()
        print(f"An error occurred during CSV loading: {e}")
        raise
    finally:
        db.close()