from datetime import datetime, date
from selenium import webdriver
import time
from selenium.webdriver.support.select import Select
import psycopg2

hb_cliente_id = 1 # Cliente "vinx". tabela hb_cliente.id

class Hb_midia_lote:
  id = 0
  datahorainicio = ''
  datahorafim = ''
  pagina_final = 0
  linhas_lidas = 0
  linhas_repetidas = 0
  linhas_inseridas = 0
  ultimas_inseridas = 0
  textofinalizacao = ''

  def Start(self):
    record = conexao.Query('insert into hb_midia_lote(hb_cliente_id, datahorainicio) values (' + str(hb_cliente_id) + ', now()) returning id, datahorainicio')
    self.id = record[0]
    self.datahorainicio = record[1]

  def savePostGres(self, textofinalizacao):
    self.textofinalizacao = textofinalizacao
    query = "update hb_midia_lote set " + \
             "datahorafim = now()" + \
             ", pagina_final = " + str(self.pagina_final) + \
             ", linhas_lidas = " + str(self.linhas_lidas) + \
             ", linhas_repetidas = " + str(self.linhas_repetidas) + \
             ", linhas_inseridas = " + str(self.linhas_inseridas) + \
             ", ultimas_inseridas = " + str(self.ultimas_inseridas) + \
             ", textofinalizacao = '" + str(self.textofinalizacao) + \
             "' where id = " + str(self.id) + \
             " returning datahorafim"

    record = conexao.Query(query)
    self.datahorafim = record[0]

class Conexao:
  connection = psycopg2.connect(user="postgres",
                                password="Charisma2022",
                                host="177.92.115.138",
                                port="5432",
                                database="charisma")
  cursor = connection.cursor()
  hb_midia_lote = Hb_midia_lote()

  def __init__(self):
    self.connection.autocommit = True

  def Query(self, Sql):
    self.cursor.execute(Sql)
    record = self.cursor.fetchone()
    return record

  def ExecSql(self, Sql):
    self.cursor.execute(Sql)

class recordClass:
  Linha = 0
  Pagina = 0
  DataAcesso = '' # a data está no formato '29/Feb/2020'
  Cliente = ''
  Momento = ''
  Canal = ''
  Midia = ''
  Corretor = ''
  Gerente = ''
  Produto = ''
  Regional = ''
  TipoMidia = ''

  def DataAcesso_Date(self):
    return datetime.strptime(self.DataAcesso, '%d/%b/%Y') # a data está no formato '29/Feb/2020'

  def DataAcesso_Sql(self):
    dt = self.DataAcesso_Date()
    dtSql = '{:%Y-%m-%d}'.format(dt)
    return dtSql

  def getText(self):
    return 'Pagina: ' + str(self.Pagina) + \
    ' - Linha: ' + str(self.Linha) + \
    ' - DataAcesso: ' + self.DataAcesso + \
    ' - Cliente: ' + self.Cliente + \
    ' - Momento: ' + self.Momento + \
    ' - Canal: ' + self.Canal + \
    ' - Midia: ' + self.Midia + \
    ' - Corretor: ' + self.Corretor + \
    ' - Gerente: ' + self.Gerente + \
    ' - Produto: ' + self.Produto + \
    ' - Regional: ' + self.Regional + \
    ' - TipoMidia: ' + self.TipoMidia

  def insertPostgres(self, cursor):
    try:
      record_str = self.getText()

      def StrSql(value):
        return "'" + value + "'" + ','

      query = 'select func_insert_hb_midia(' + \
        str(conexao.hb_midia_lote.id) + ', ' + \
        str(self.Pagina) + ', ' + \
        str(self.Linha) + ', ' + \
        StrSql(self.DataAcesso_Sql()) + \
        StrSql(self.Cliente) + \
        StrSql(self.Momento) + \
        StrSql(self.Canal) + \
        StrSql(self.Midia) + \
        StrSql(self.Corretor) + \
        StrSql(self.Gerente) + \
        StrSql(self.Produto) + \
        StrSql(self.Regional) + \
        "'" + self.TipoMidia + "')"

      cursor.execute(query)
      record = cursor.fetchone()
      insert_return = record[0]
      print('[' + insert_return + '] ' + record_str)

      conexao.hb_midia_lote.linhas_lidas += 1

      if 'Já existe - id:' in insert_return:
        conexao.hb_midia_lote.linhas_repetidas += 1
        conexao.hb_midia_lote.ultimas_inseridas = 0
      else:
        conexao.hb_midia_lote.linhas_inseridas += 1
        conexao.hb_midia_lote.ultimas_inseridas += 1

      return record[0]

    except (Exception, psycopg2.Error) as error:
        print('--------------------------')
        print("Erro:", record_str)
        print(error)
        print('--------------------------')

def getaDataTable(page):
  #Esta rotina coleta os dados da paginação atual do browser e joga estes dados em <records>
  table_data = driver.find_element_by_id("tabela_relatorio_midias")

  rows = table_data.find_elements_by_tag_name("tr")  # get all of the rows in the table
  lineTable = 0

  for row in rows[2:]:  # Os 2 primeiros registros fazem parte do cabeçalho
    # print('Coletando Página: ' + str(page) + ', linha: ' + str(lineTable))
    lineTable += 1

    # Get the columns (all the column 2)
    cols = row.find_elements_by_class_name("coluna")

    def getColumnVal(index1, secondValue = False, index2 = 1):
      col = cols[index1].find_elements_by_tag_name('p')

      if secondValue:
        return col[0].text, col[index2].text
      else:
        return col[0].text

    record = recordClass()

    record.Linha = lineTable
    record.Pagina = page

    record.DataAcesso = getColumnVal(0)
    record.Cliente, record.Momento = getColumnVal(1, True)
    record.Canal, record.Midia = getColumnVal(2, True, 2)
    record.Corretor, record.Gerente = getColumnVal(3, True)
    record.Produto, record.Regional = getColumnVal(4, True)
    record.TipoMidia = getColumnVal(5)

    res = record.insertPostgres(cursor)

    # if lineTable >= 4: #comentar estas duas linhas
    #   break

  return

#########
#Início #
#########

try:
  # Abrindo Navegador, abrindo conexões...

  conexao = Conexao()
  conexao.hb_midia_lote.Start() #Inicia o Lote
  cursor = conexao.cursor
  conexao.ExecSql('delete from hb_midia h where h.dataacesso >= current_date - 30') # Deletar últimos 30 dias para reprocessar todos novamente

  driver = webdriver.Chrome(executable_path=r'../chromedriver.exe')

  driver.get("https://vinx.hypnobox.com.br/")
  time.sleep(2)
  driver.find_element_by_id("hypno_txt_login_email").send_keys('charisma@docsync.com.br')
  driver.find_element_by_id("hypno_txt_login_senha").send_keys('48tmXvNA')

  driver.find_element_by_id("login").click()

  time.sleep(2)

  driver.get("https://vinx.hypnobox.com.br/?controle=Relatorio&acao=midia")

  time.sleep(2)
  select_element = driver.find_element_by_id('btntotalporpagina_form_filtro_midias')
  select_object = Select(select_element)
  select_object.select_by_value('100') # Setar para "100 itens por página"
  #select nextval('hb_midia_lote')

  time.sleep(2)

  records = []

  def getCurrentPage():
    botaoPaginacao = driver.find_element_by_xpath("//a[@class='botao_paginacao paginacao_eventos_form_filtro_midias ativo']")
    return int(botaoPaginacao.text)

  nextPage = 1

  ###############################################################################################
  #Navegando pelas páginas para chamar rotina "getaDataTable" que coleta os dados do html-table #
  ###############################################################################################

  while True:
    currentPage = getCurrentPage()
    if nextPage != currentPage: #Ou Chegou na últimqa página(Opção padrão), ou alguma coisa não saiu conforme esperado.
      break

    qtdeLinhasInseridasAntesPaginaAtual = conexao.hb_midia_lote.linhas_inseridas
    getaDataTable(currentPage)

    if qtdeLinhasInseridasAntesPaginaAtual == conexao.hb_midia_lote.linhas_inseridas:
      # Na última página processada não inseri nenhum linha. Neste caso vou parar o processo
      print('Encerrando na página "' + str(currentPage) + '" pois nenhum registro foi inserido')
      break

    # Muda para a próxima página
    nextpage_button = driver.find_elements_by_xpath("//a[@class='proximo paginacao_eventos_form_filtro_midias']")
    nextpage_button[0].click()
    time.sleep(3)

    nextPage += 1

    # if nextPage >= 1:  # comentar estas duas linhas
    #   break

  conexao.hb_midia_lote.pagina_final = currentPage
  #Finaliza o processamento fechando o lote(tabela hb_midia_lote)
  conexao.hb_midia_lote.savePostGres('normal')
  print("")
  print("** Processamento efetuado com sucesso! **")

finally:
    # closing database connection.
    if conexao.connection:
        cursor.close()
        conexao.connection.close()
        print("PostgreSQL connection is closed")
