import sqlite3

conn = sqlite3.connect('export_brain.db')
c = conn.cursor()

# Table for General Rules (CBAM, Export Bans, etc.)
c.execute('''CREATE TABLE IF NOT EXISTS RegulationMaster 
             (hs_code TEXT, country TEXT, rule_type TEXT, details TEXT, date_updated TEXT)''')

# Table for Safety/Chemical Limits (Crucial for 2026 Textile Laws)
c.execute('''CREATE TABLE IF NOT EXISTS ChemicalLimits 
             (hs_code TEXT, substance TEXT, max_limit REAL, source TEXT)''')

conn.commit()
conn.close()
print("Brain Initialized.")
