
def convert_to_lower_case(df):
    return df.transform(lambda x: x.str.lower())

def insert_underscore_for_space(df):
    return df.transform(lambda x: x.str.replace(' ', '_'))
