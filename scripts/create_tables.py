import common.base as base
import common.tables

# Create the table in the database
if __name__ == "__main__":
    base.Base.metadata.create_all(base.engine, checkfirst=True)
