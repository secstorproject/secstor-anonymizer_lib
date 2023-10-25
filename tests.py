from lib.encryption import *
from lib.generalization import *
from lib.hashing import *
from lib.masking import *
from lib.null_out import *
from lib.perturbation import *
from lib.pseudonymization import *
from lib.swapping import *
from utils.data_processing import *
import threading

#dados iniciais
data = [
        {
            "nome": "João",
            "sobrenome": "Mitchard",
            "email": "Wain@gmail.com",
            "cpf": "12345678901",
            "data": "01/01/2001",
            "idade": 10
        },
        {
            "nome": "Mariam",
            "email": "Mariam@gmail.com",
            "cpf": "123.456.789-02",
            "data": "01/01/2002",
            "idade": 15
        },
        {
            "nome": "Pedro",
            "sobrenome": "Gambles",
            "email": "Amalea@gmail.com",
            "cpf": "12345678903",
            "data": "01-01-2003",
            "idade": 20
        },
        {
            "nome": "João",
            "sobrenome": "Mitchard",
            "email": "Wain@gmail.com",
            "cpf": "12345678901",
            "data": "01-01-2004",
            "idade": 19
        },
        {
            "nome": "Pedro",
            "sobrenome": "Gambles",
            "email": "Amalea@gmail.com",
            "cpf": "12345678903",
            "data": "01-01-2005",
            "idade": 11
        }
    ]

# DataFrame exemplo
df = value_to_dataframe(data)

# Criação do semáforo para sincronização
semaphore = threading.Semaphore()

convert_to_datetime(df, ['data'], semaphore)

# Métodos OK:
#generalization(df, ['idade'], age_generalize_func, semaphore)
#generalization(df, ['idade'], percent_generalize_func, semaphore)
#apply_md5(df, ['nome', 'sobrenome'], semaphore)
#apply_sha1(df, ['nome', 'sobrenome'], semaphore)
#apply_sha256(df, ['nome', 'sobrenome'], semaphore)
#perturb_date(df, ['data'], 'days', -10, 10, semaphore)
#perturb_date(df, ['data'], 'hours', -10, 10, semaphore)
#perturb_date(df, ['data'], 'minutes', -10, 10, semaphore)
#perturb_date(df, ['data'], 'seconds', -10, 10, semaphore)
#perturb_date(df, ['data'], 'milliseconds', -10, 10, semaphore)
#perturb_date(df, ['data'], 'microseconds', -10, 10, semaphore)
#perturb_date(df, ['data'], 'nanoseconds', -10, 10, semaphore)
#perturb_numeric_gaussian(df, ['idade'], 5, semaphore)
#pseudonymize_columns(df, ['nome', 'sobrenome'], semaphore)
#swap_columns(df, ['nome', 'sobrenome'], semaphore)
#swap_rows(df, ['nome', 'sobrenome'], semaphore)

#Métodos incompletos:
#encrypt_chacha20(df, 'nome', 'teste', semaphore) #[falta trocar uma coluna por array de colunas]
#encrypt_salsa20(df, 'nome', 'teste', semaphore) #[falta trocar uma coluna por array de colunas]
#encrypt_aes(df, 'nome', 'teste', semaphore) #[falta trocar uma coluna por array de colunas]
#df = drop_columns(df, ['nome', 'sobrenome'], semaphore) #[revisar para evitar possíveis problemas]
#perturb_numeric_laplacian(df, ['idade'], 0.1, semaphore) #[estudar se só funciona para float]

# Métodos para correção:
#mask_full(df,['idade'], semaphore)
#mask_range(df, ['nome', 'sobrenome'], 1, 2, semaphore)
#mask_last_n_characters(df, ['nome', 'sobrenome'], 3, semaphore)
#mask_first_n_characters(df, ['nome', 'sobrenome'], 3, semaphore)
#mask_email(df, ['email', 'sobrenome'], semaphore)
#mask_cpf(df, 'cpf', semaphore)
#perturb_numeric_range(df, ['idade'], 5, semaphore)
#pseudonymize_rows(df, ['nome', 'sobrenome'], semaphore)

# Visualização do DataFrame
print(df)