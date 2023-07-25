#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>

using namespace std;
ofstream resultados("resultados.csv");
int NUM_THREADS = 1; //define numero de threads

// Estrutura para armazenar os dados do arquivo CSV
struct Movie {
    string titulo;
    string avaliacao;
    string genero;
    int ano;
    string lancamento;
    float pontuacao;
    int votos;
    string diretor;
    string escritor;
    string elenco;
    string pais;
    int orcamento;
    int bruto;
    string produtora;
    string tempo;
};

// Função para converter string em int
int stringParaint(const string& str) {
    int resultado = 0;
    size_t i = 0;

    for (; i < str.length(); ++i) {
        char digito = str[i];
        if (digito >= '0' && digito <= '9') {
            resultado = resultado * 10 + (digito - '0');
        } else {
            
            break;
        }
    }

    return resultado;
}

// Função para dividir as linhas do arquivo CSV
vector<string> divisaoCSVlinhas(const string& line) {
    vector<string> resultado;
    stringstream ss(line);
    string field;
    // separa os itens por virgula e se tiver dados entre aspas, ele junta e ignora virgulas
    while (getline(ss, field, ',')) {
        if (!field.empty() && field.front() == '"' && field.back() != '"') {
            string tempField;
            while (getline(ss, tempField, ',')) {
                field += "," + tempField;
                if (tempField.back() == '"')
                    break;
            }
        }
        // remove as aspas
        resultado.push_back(field);
    }

    return resultado;
}

vector<Movie> armazena_vetor(int n_linhas){
    // abre o arquivo
    ifstream file("filmes.csv");
    if (!file) {
        cerr << "Erro ao abrir o arquivo." << endl;
    }
    
    // cria um vetor de filmes
    vector<Movie> vetor;
    string line;

    // remove a primeira linha
    getline(file, line);

    // le as linhas restantes
    while (getline(file, line) and n_linhas > 0) {
        vector<string> fields = divisaoCSVlinhas(line);

        Movie movie;
        movie.titulo = fields[0];
        movie.avaliacao = fields[1];
        movie.genero = fields[2];
        movie.ano = stringParaint(fields[3]);
        movie.lancamento = fields[4];
        movie.pontuacao = stof(fields[5]);
        movie.votos = stringParaint(fields[6]);
        movie.diretor = fields[7];
        movie.escritor = fields[8];
        movie.elenco = fields[9];
        movie.pais = fields[10];
        movie.orcamento = stringParaint(fields[11]);
        movie.bruto = stringParaint(fields[12]);
        movie.produtora = fields[13];
        movie.tempo = fields[14];

        vetor.push_back(movie);
        n_linhas --;
    }

    return vetor;

}

void gera_csv_ordenado(vector<Movie> vetorOrdenado){
    string nomeArquivo = "filmesOrdenados.csv";

    // Abre o arquivo para escrita
    ofstream arquivo(nomeArquivo);

    // Verifica se o arquivo foi aberto corretamente
    if (!arquivo) {
        cerr << "Erro ao abrir o arquivo." <<endl;
    }else{
        //cabeçalho
        arquivo << "Título,Avaliação,Gênero,Ano,Lançamento,Pontuação,Votos,Diretor,Escritor,Elenco,País,Orçamento,Bruto,Produtora,Tempo" << std::endl;
        // Escreve os elementos do vetor no arquivo CSV
        for (const auto& filme : vetorOrdenado) {
            arquivo << filme.titulo << ","
                    << filme.avaliacao << ","
                    << filme.genero << ","
                    << filme.ano << ","
                    << filme.lancamento << ","
                    << filme.pontuacao << ","
                    << filme.votos << ","
                    << filme.diretor << ","
                    << filme.escritor << ","
                    << filme.elenco << ","
                    << filme.pais << ","
                    << filme.orcamento << ","
                    << filme.bruto << ","
                    << filme.produtora << ","
                    << filme.tempo << endl;
        }
        // Fecha o arquivo
        arquivo.close();

        cout << "CSV gerado com sucesso." << endl;
    }

    
}

// atribui valor ao vetor de filmes
vector<Movie> vetor = armazena_vetor(4000);

// define os dados de inicialização
int SEMENTE = 100;
int TAMANHO = vetor.size() ;

int NUMERO_POR_THREADS = TAMANHO / NUM_THREADS;
int OFFSET = TAMANHO % NUM_THREADS;

// Função responsável por combinar duas metades ordenadas do vetor
void merge(vector<Movie>, int esquerda, int meio, int direita) {
    int i = 0;
    int j = 0;
    int k = 0;
    int tamanho_esq = meio - esquerda + 1;
    int tamanho_dir = direita - meio;
    Movie vetor_esq[tamanho_esq];
    Movie vetor_dir[tamanho_dir];
    
    for (int i = 0; i < tamanho_esq; i ++) {
        vetor_esq[i] = vetor[esquerda + i];
    }
    
    for (int j = 0; j < tamanho_dir; j ++) {
        vetor_dir[j] = vetor[meio + 1 + j];
    }
    
    i = 0;
    j = 0;

    while (i < tamanho_esq && j < tamanho_dir) {
        if (vetor_esq[i].pontuacao <= vetor_dir[j].pontuacao) {
            vetor[esquerda + k] = vetor_esq[i];
            i ++;
        } else {
            vetor[esquerda + k] = vetor_dir[j];
            j ++;
        }
        k ++;
    }
    
    while (i < tamanho_esq) {
        vetor[esquerda + k] = vetor_esq[i];
        k ++;
        i ++;
    }

    while (j < tamanho_dir) {
        vetor[esquerda + k] = vetor_dir[j];
        k ++;
        j ++;
    }
}

// Função para fazer o merge sort
void merge_sort(vector<Movie>, int esquerda, int direita) {
    if (esquerda < direita) {
        int meio = esquerda + (direita - esquerda) / 2;
        merge_sort(vetor, esquerda, meio);
        merge_sort(vetor, meio + 1, direita);
        merge(vetor, esquerda, meio, direita);
    }
}

//  Essa função é usada para juntar as partes do vetor que foram ordenadas pelas threads.
void merge_parte_vetor(vector<Movie>, int numero, int agregacao) {
    for(int i = 0; i < numero; i = i + 2) {
        int esquerda = i * (NUMERO_POR_THREADS * agregacao);
        int direita = ((i + 2) * NUMERO_POR_THREADS * agregacao) - 1;
        int meio = esquerda + (NUMERO_POR_THREADS * agregacao) - 1;
        if (direita >= TAMANHO) {
            direita = TAMANHO - 1;
        }
        merge(vetor, esquerda, meio, direita);
    }
    if (numero / 2 >= 1) {
        merge_parte_vetor(vetor, numero / 2, agregacao * 2);
    }
}

// chamada pelas threads para realizar a ordenação em paralelo
void *thread_merge_sort(void* arg){
    int thread_id = (long)arg;
    int esquerda = thread_id * (NUMERO_POR_THREADS);
    int direita = (thread_id + 1) * (NUMERO_POR_THREADS) - 1;
    if (thread_id == NUM_THREADS - 1) {
        direita += OFFSET;
    }
    int meio = esquerda + (direita - esquerda) / 2;
    if (esquerda < direita) {
        merge_sort(vetor, esquerda, direita);
        merge_sort(vetor, esquerda + 1, direita);
        merge(vetor, esquerda, meio, direita);
    }
    return NULL;
}

// Função principal que executa as threads
void funcao_pthread(){
    srand(SEMENTE);
    struct timeval  inicio, fim;
    double tempo_gasto;
    
    //inicia o tempo
    pthread_t threads[NUM_THREADS];
    gettimeofday(&inicio, NULL);
    
    //Criando as threads
    for (long i = 0; i < NUM_THREADS; i ++) {
        int rc = pthread_create(&threads[i], NULL, thread_merge_sort, (void *)i);
        if (rc){
            exit(-1);
        }
    }
    // Espera as threads terminarem
    for(long i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Faz o merge das partes ordenadas
    merge_parte_vetor(vetor, NUM_THREADS, 1);
    
   // finaliza o tempo
    gettimeofday(&fim, NULL);
    tempo_gasto = ((double) ((double) (fim.tv_usec - inicio.tv_usec) / 1000000 +
                            (double) (fim.tv_sec - inicio.tv_sec)));

    // imprime o tempo de execução                   
    cout << "Tempo de execução " <<  tempo_gasto <<endl;
    resultados << "," << tempo_gasto << endl;
}

int main(int argc, const char * argv[]) {
    resultados << "Tamanho do vetor, Tempo de Execução (s)" << endl;
    
    for(int i = 1; i < 10; i++){
        // varia o tamanho de dados captados do arquivo, assim como as variaveis para as threads
        vetor = armazena_vetor(4000);
        NUM_THREADS = i;
        NUMERO_POR_THREADS = TAMANHO / NUM_THREADS;
        OFFSET = TAMANHO % NUM_THREADS;
        resultados << i;
        funcao_pthread();
        gera_csv_ordenado(vetor);
        vetor.clear();
    }
    resultados.close();

    return 0;
}




