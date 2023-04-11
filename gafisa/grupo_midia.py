from datetime import datetime, date, timedelta
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By

import time
from selenium.webdriver.support.select import Select
import psycopg2

hb_cliente_id = 2 # Gafisa - tabela hb_cliente.id

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

  def log(self, tipolog, mensagem):
    mensagem = mensagem.replace("'", "''")
    query = "insert into hb_lote_log(hb_midia_lote_id, tipolog, mensagem) values (" + str(self.hb_midia_lote.id) + ", '"\
            + tipolog + "', '" + mensagem + "')"
    self.ExecSql(query)

class recordClass:
  id = 0
  pagina = 0
  linha = 0
  create_datetime = ''
  hb_midia_lote_id = ''

  data = ''
  grupomidia = ''
  totalinvestido = 0
  pesquisa = 0
  visita = 0
  proposta = 0
  venda = 0
  retorno = 0


  def dataToTimeStampSql(self, data):
    if (data.strip() == ''):
      return 'null'

    dt = datetime.strptime(data, '%d/%m/%Y')
    dtSql = '{:%Y-%m-%d}'.format(dt)
    return "'" + dtSql + "'"

  def datacadastroToTimeStamp(self):
    return self.dataToTimeStampSql(self.data)


  def DataAcesso_Date(self):
    return datetime.strptime(self.DataAcesso, '%d/%b/%Y') # a data está no formato '29/Feb/2020'

  def DataAcesso_Sql(self):
    dt = self.DataAcesso_Date()
    dtSql = '{:%Y-%m-%d}'.format(dt)
    return dtSql

  def getText(self):
    return \
      'pagina: ' + str(self.pagina) + \
      ' - linha: ' + str(self.linha) + \
      ' - grupomidia: ' + str(self.grupomidia) + \
      ' - totalinvestido: ' + str(self.totalinvestido) + \
      ' - pesquisa: ' + str(self.pesquisa) + \
      ' - visita: ' + str(self.visita) + \
      ' - proposta: ' + str(self.proposta) + \
      ' - venda: ' + str(self.venda) + \
      ' - retorno: ' + str(self.retorno) + \
      ' - create_datetime: ' + str(self.create_datetime) + \
      ' - hb_midia_lote_id: ' + str(self.hb_midia_lote_id)

  def insertPostgres(self, cursor):
    try:
      record_str = self.getText()

      # if (self.email == 'nairjhf@hotmail.com'):
      # # if (self.email == 'karina.cbgomes@gmail.com'):
      #   print('x')

      def StrSql(value):
        value = value.replace("'", "''")
        return "'" + value + "'"

      def valSql(value):
        if value == None:
          return 'null'
        else:
          return value

      query = 'select func_insert_hb_grupomidia(' + \
        str(self.hb_midia_lote_id) + ', ' + \
        str(self.linha) + ', ' + \
        str(self.pagina) + ', ' + \
        self.datacadastroToTimeStamp() + ', ' + \
        StrSql(self.grupomidia) + ', ' + \
        valSql(self.totalinvestido) + ', ' + \
        valSql(self.pesquisa) + ', ' + \
        valSql(self.visita) + ', ' + \
        valSql(self.proposta) + ', ' + \
        valSql(self.venda) + ', ' + \
        valSql(self.retorno) + ")"

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
        print("Erro-insert:", record_str)
        print(error)
        print('--------------------------')
        conexao.log('erro-insert-record_str', record_str)
        conexao.log('erro-insert-error', str(error))

def getaDataTable(currentDate):
  #Esta rotina coleta os dados da paginação atual do browser e joga estes dados em <records>

  print('')
  print('### Processando data: "', currentDate, '" ###')
  wait.until_not(ec.visibility_of_element_located((By.XPATH, "//table[@class='loader']")))

  table_data = driver.find_element_by_xpath("//table[@class='tabela-padrao2 persist-area']")

  rows = table_data.find_elements_by_tag_name("tr")  # get all of the rows in the table
  lineTable = 0

  for row in rows[2:]:  # Os 2 primeiros registros fazem parte do cabeçalho
    # print('Coletando Página: ' + str(page) + ', linha: ' + str(lineTable))
    lineTable += 1
    rowHtml = [row.get_attribute('innerHTML')] # O único motivo de ser um array para esta variável é porque por enquanto foi a única maneira que encontrei de deixar esta variável publica
    rowHtml_start = [row.get_attribute('innerHTML')]
    cols = row.find_elements_by_tag_name("td")

    def deletarStrAte(str):
      strPos = rowHtml[0].find(str)
      if strPos == -1:
        return False

      strPos += len(str)

      rowHtml[0] = rowHtml[0][strPos:]
      return True


    def getColumnVal(palavraAntes, palavraDepois):
      if palavraAntes != '':
        deletarStrAte(palavraAntes)
      palavraDepoisPos = rowHtml[0].find(palavraDepois)
      retorno = rowHtml[0][:palavraDepoisPos].strip()
      deletarStrAte(palavraDepois)
      return retorno

    currentCol = [0]

    def getNextValue():
      rowHtml[0] = cols[currentCol[0]].get_attribute('innerHTML')
      deletarStrAte('<p class=')
      currentCol[0] += 1
      res = getColumnVal('>', '</p>')
      if res == 'R$ 0,00':
        res = '0'

      if res == 'R$   ---':
        res = None

      return res


    record = recordClass()

    record.data = currentDate
    record.hb_midia_lote_id = conexao.hb_midia_lote.id
    record.linha = lineTable
    record.pagina = 0

    record.grupomidia = getNextValue()
    record.totalinvestido = getNextValue()
    record.pesquisa = getNextValue()
    record.visita = getNextValue()
    record.proposta = getNextValue()
    record.venda = getNextValue()
    record.retorno = getNextValue()

    record.insertPostgres(cursor)

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
  conexao.ExecSql('delete from hb_grupomidia h where h.data >= current_date - 30') # Deletar últimos 30 dias para reprocessar todos novamente

  driver = webdriver.Chrome(executable_path=r'../chromedriver.exe')

  wait = WebDriverWait(driver, 10)

  driver.get("https://gafisa.hypnobox.com.br")
  time.sleep(2)
  driver.find_element_by_id("hypno_txt_login_email").send_keys('charisma@docsync.com.br')
  driver.find_element_by_id("hypno_txt_login_senha").send_keys('G@fisa22')
  driver.find_element_by_id("login").click()

  time.sleep(2)

  driver.get("https://gafisa.hypnobox.com.br/?controle=Relatorio&acao=mediaReport")

  time.sleep(3)

  records = []


  ###############################################################################################
  #Navegando pelas datas #
  ###############################################################################################

  currentDate = datetime.today().date()
  # currentDate = datetime.strptime('2022-01-01', '%Y-%m-%d').date()
  currentDate += timedelta(days=-31)

  # startDate = datetime.today().date() - timedelta(days=30) # Refazer os últimos 30 dias para pegar possíveis mudanças nos dados do minter

  while currentDate <= datetime.today().date():

    filtro_periodo = driver.find_element_by_id("filtro-periodo-relatorios")

    inputs_filtro_data = filtro_periodo.find_elements_by_tag_name("input")  #

    currentDateStr = '{:%d/%m/%Y}'.format(currentDate)

    driver.execute_script('''
        var datade = arguments[0];
        var dataate = arguments[1];
        var value = arguments[2];
        datade.value = value;
        dataate.value = value;
    ''', inputs_filtro_data[0], inputs_filtro_data[1], currentDateStr)

    driver.find_element_by_id("btn_gerar").click()
    time.sleep(1)

    getaDataTable(currentDateStr)


    # Muda para a próxima página
    # currentDate = datetime.today().date()
    currentDate += timedelta(days=1)


    # if nextPage >= 1:  # comentar estas duas linhas
    #   break

  #Finaliza o processamento fechando o lote(tabela hb_midia_lote)
  # conexao.hb_midia_lote.savePostGres('normal')

  print("")
  print("** Processamento efetuado com sucesso! **")

finally:
    # closing database connection.
    if conexao.connection:
        cursor.close()
        conexao.connection.close()

