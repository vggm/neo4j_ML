import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules

# Ejemplo de datos: lista de transacciones
dataset = pd.read_csv('./BestBooksEverClean_train_dataset.csv', index_col=None)

# Transformación de los datos para el formato adecuado
te = TransactionEncoder()
te_ary = te.fit(dataset).transform(dataset)
df = pd.DataFrame(te_ary, columns=te.columns_)

# Aplicación del algoritmo Apriori
frequent_itemsets = apriori(df, min_support=0.5, use_colnames=True)

# Generar las reglas de asociación
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.7)

# Mostrar las reglas de asociación
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']])
