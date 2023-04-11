from datetime import datetime, date
from selenium import webdriver
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
  nome = ''
  email = ''
  telefone = ''
  datacadastro = '' # data de cadastro está no formato "27/04/2022 as 11:19"
  dataatualizacao = '' # data de cadastro está no formato "27/04/2022 as 11:19"
  corretor = ''
  gerente = ''
  produtoatual = ''
  canalorigem = ''
  subcanal = ''
  momento = ''
  submomento = ''
  statusofertaativa = ''
  termometro = ''
  create_datetime = ''
  hb_midia_lote_id = ''

  def dataToTimeStampSql(self, data):
    if (data.strip() == ''):
      return 'null'

    data_split = data.split('às')
    data = data_split[0]
    hora = data_split[1]

    dt = datetime.strptime(data + ' ' + hora, '%d/%m/%Y %H:%M')
    dtSql = '{:%Y-%m-%d %H:%M}'.format(dt)
    return "'" + dtSql + "'"

  def datacadastroToTimeStamp(self):
    return self.dataToTimeStampSql(self.datacadastro)

  def dataatualizacaoToTimeStamp(self):
    return self.dataToTimeStampSql(self.dataatualizacao)


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
      ' - nome: ' + str(self.nome) + \
      ' - email: ' + str(self.email) + \
      ' - telefone: ' + str(self.telefone) + \
      ' - datacadastro: ' + str(self.datacadastro) + \
      ' - dataatualizacao: ' + str(self.dataatualizacao) + \
      ' - corretor: ' + str(self.corretor) + \
      ' - gerente: ' + str(self.gerente) + \
      ' - produtoatual: ' + str(self.produtoatual) + \
      ' - canalorigem: ' + str(self.canalorigem) + \
      ' - subcanal: ' + str(self.subcanal) + \
      ' - momento: ' + str(self.momento) + \
      ' - submomento: ' + str(self.submomento) + \
      ' - statusofertaativa: ' + str(self.statusofertaativa) + \
      ' - termometro: ' + str(self.termometro) + \
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
        return "'" + value + "'" + ','

      query = 'select func_insert_hb_cliente_rep(' + \
        str(self.hb_midia_lote_id) + ', ' + \
        str(self.linha) + ', ' + \
        str(self.pagina) + ', ' + \
        StrSql(self.nome) + \
        StrSql(self.email) + \
        StrSql(self.telefone) + \
        self.datacadastroToTimeStamp() + ', ' + \
        self.dataatualizacaoToTimeStamp() + ', ' + \
        StrSql(self.corretor) + \
        StrSql(self.gerente) + \
        StrSql(self.produtoatual) + \
        StrSql(self.canalorigem) + \
        StrSql(self.subcanal) + \
        StrSql(self.momento) + \
        StrSql(self.submomento) + \
        StrSql(self.statusofertaativa) + \
        "'" + self.termometro + "')"

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

def getaDataTable(page):
  #Esta rotina coleta os dados da paginação atual do browser e joga estes dados em <records>
  # if page < 15:
  #   print('!!!!escapando página menor que 14')
  #   return

  table_data = driver.find_element_by_id("tabela_cliente")

  rows = table_data.find_elements_by_tag_name("tr")  # get all of the rows in the table
  lineTable = 0

  for row in rows[2:]:  # Os 2 primeiros registros fazem parte do cabeçalho
    # print('Coletando Página: ' + str(page) + ', linha: ' + str(lineTable))
    lineTable += 1
    rowHtml = [row.get_attribute('innerHTML')] # O único motivo de ser um array para esta variável é porque por enquanto foi a única maneira que encontrei de deixar esta variável publica
    rowHtml_start = [row.get_attribute('innerHTML')]
    cols = row.find_elements_by_class_name("coluna")

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

    def setCol(colNum):
      rowHtml[0] = cols[colNum].get_attribute('innerHTML')

    record = recordClass()

    record.hb_midia_lote_id = conexao.hb_midia_lote.id
    record.linha = lineTable
    record.pagina = page

    setCol(0)
    record.termometro = getColumnVal('ico-legenda', '"><')
    setCol(1)
    deletarStrAte('<p class=')
    record.nome = getColumnVal('">', '</p>')
    record.email = getColumnVal('">', '</p>')

    # if record.email != 'solankisp99@yahoo.com':
    #   continue
    # else:
    #   print('x')
    record.telefone = getColumnVal('">', '</p>')

    setCol(2)
    deletarStrAte('Cadastro')
    deletarStrAte('<p class=')
    record.datacadastro = getColumnVal('">', '</p>')
    if deletarStrAte('Atualização'):
      deletarStrAte('<p class=')
      record.dataatualizacao = getColumnVal('">', '</p>')
    else:
      record.dataatualizacao = record.datacadastro

    setCol(3)
    deletarStrAte('<p class=')
    record.corretor = getColumnVal('">', '</p>')
    record.gerente = getColumnVal('">', '</p>')

    setCol(4)
    deletarStrAte('<p class=')
    record.produtoatual = getColumnVal('">', '</p>')
    deletarStrAte('<p class=')
    record.canalorigem = getColumnVal('style="width:108px;">', '</p>')
    deletarStrAte('<p class=')
    record.subcanal = getColumnVal('>', '</p>')

    setCol(5)
    deletarStrAte('<p class=')
    record.momento = getColumnVal('">', '</p>')

    record.submomento = getColumnVal('class="f-11">', '</p>')

    setCol(6)
    deletarStrAte('<p class=')
    record.statusofertaativa = getColumnVal('">', '</p>')
    res = record.insertPostgres(cursor)
    time.sleep(0)

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
  # conexao.ExecSql('delete from hb_cliente_rep h where h.dataatualizacao >= current_date - 30') # Deletar últimos 30 dias para reprocessar todos novamente

  driver = webdriver.Chrome(executable_path=r'../chromedriver.exe')

  driver.get("https://gafisa.hypnobox.com.br")
  time.sleep(2)
  driver.find_element_by_id("hypno_txt_login_email").send_keys('charisma@docsync.com.br')
  driver.find_element_by_id("hypno_txt_login_senha").send_keys('G@fisa22')
  driver.find_element_by_id("login").click()

  time.sleep(2)

  driver.get("https://gafisa.hypnobox.com.br/?controle=Cliente&acao=index")

  time.sleep(4)
  select_element = driver.find_element_by_id('btntotalporpagina_form_filtro_cliente')
  select_object = Select(select_element)
  select_object.select_by_value('100') # Setar para "100 itens por página"
  #select nextval('hb_midia_lote')

  time.sleep(4)

  records = []

  def getCurrentPage():
    botaoPaginacao = driver.find_element_by_xpath("//a[@class='botao_paginacao paginacao_eventos_form_filtro_cliente ativo']")
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
    nextpage_button = driver.find_elements_by_xpath("//a[@class='proximo paginacao_eventos_form_filtro_cliente']")
    nextpage_button[0].click()
    time.sleep(4)

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
