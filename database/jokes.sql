-- schema.sql
   CREATE TABLE IF NOT EXISTS jokes (
       id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Added id as primary key
       category TEXT,
       type TEXT,
       joke TEXT,
       setup TEXT,
       delivery TEXT,
       nsfw BOOLEAN,
       political BOOLEAN,
       sexist BOOLEAN,
       safe BOOLEAN,
       lang TEXT
   );