# Padrões Básicos de Python

## Estilo de Código

### Nomes e Convenções
- Use `snake_case` para variáveis, funções e métodos
- Use `PascalCase` para classes
- Use `UPPER_CASE_WITH_UNDERSCORES` para constantes
- Nomes de métodos privados e variáveis devem começar com underscore (`_private_var`)

### Indentação
- Use 4 espaços para indentação (não tabs)
- Limite linhas a 79 caracteres para código e 72 para comentários

### Imports
- Imports devem estar em linhas separadas
- Agrupar imports na seguinte ordem:
    1. Bibliotecas padrão
    2. Bibliotecas de terceiros
    3. Imports locais
- Use imports absolutos quando possível

## Boas Práticas

### Documentação
- Use docstrings para funções, classes e módulos
- Siga o formato de docstring PEP 257

```python
def exemplo(parametro):
        """Breve descrição da função.
        
        Descrição detalhada se necessária.
        
        Args:
                parametro: Descrição do parâmetro
                
        Returns:
                Descrição do retorno
        """
```

### Estrutura de Código
- Evite funções com mais de 30 linhas
- Evite nível de indentação maior que 4
- Use list comprehensions em vez de loops simples quando apropriado
- Use espaço em branco para melhorar a legibilidade

### Gerenciamento de Erros
- Use blocos try/except para tratamento de exceções
- Capture exceções específicas, não use `except:` sem qualificar
- Use `finally` para limpeza de recursos

### Testes
- Escreva testes unitários para suas funções
- Mantenha cobertura de testes acima de 80%

## PEPs Importantes
- PEP 8: Guia de estilo para código Python
- PEP 257: Convenções de Docstring
- PEP 484: Type Hints