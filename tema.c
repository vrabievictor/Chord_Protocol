#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <wait.h>
#include <openssl/sha.h>
#include <math.h>
#include <pthread.h>
/*
coduri operatii
0 get predecesor of
1 get closest preceding finger
2 get succesor od
3 call and get find pred
4 call and get find succesor
5 update predecesor
6 update finger table
7 update succesor

//perechile cheie valoare reprezinta de ex
//nume prenume

comenzi pentru afisare
show_finger_table - afiseaza finger table
show_succesor
show_predecesor

*/
//cel mai bine ar fi sa folosesc prethreading  pentru ca fac economie de resurse
//trebuie sa caut corect succesorul dupa eticheta
//vezi cazul in care ai doar un singur nod si vrei sa bagi un alt nod in retea nu merge asa cum este scris

//serializare
//o sa trimit pachete de tipul
//<cod operatie pe 1 byte><4 bytes eticheta><IP[0] pe 4 bytes><IP[1] pe 4 bytes><IP[2] pe 4 bytes><IP[3] pe 4 bytes><PORT pe 2 bytes>
//in total orice mesaj transmis trebuie sa aiba o lungime de 23 bytes

uint32_t sock;//socket prin care fac accept
uint32_t sock_nou;//socketul prin care ma conectez la alte noduri pentru a interoga
uint32_t enb=1;
uint32_t length;
struct sockaddr_in adresa_nod;
uint32_t m=8;//o sa folosesc 8 biti din valoare returnata de functia hash

int next_finger=0;

pthread_mutex_t lock=PTHREAD_MUTEX_INITIALIZER;

pthread_t t,t1,t2,t3;

//cum depistez daca am cel putin un nod in retea
//incerc sa fac bind la portul 2040 si daca nu reusesc



typedef struct data
{
	uint32_t eticheta;
	uint32_t IP[4];//IP[0].IP[1].IP[2].IP[3]  este IP nodului
	uint16_t PORT;
} node;

node succesor;
node predecesor;
node this;
node n_prim;

char s[2][100][30];
char valori_proprii[100][100];
char valori_straine[100][100];
uint32_t chei_proprii[100];
uint32_t chei_straine[100];
int nr_valori_proprii;
int nr_valori_straine;

typedef struct data_finger
{
	node nod;
	uint32_t start;
} fg;

typedef struct thread_data
{
	pthread_t id;
	uint32_t client;
	node nod;
	char msg[30];
	char op;
	uint32_t k;
} _th;

_th thread[100];

fg finger_table[200];

void impacheteaza(char * msg,char cod_operatie,uint32_t eticheta,uint32_t * IP,uint16_t PORT);
void despacheteaza(char * msg,char * code_operatie,node * t_node);
void completeaza_adresa_destinatar(struct sockaddr_in * adresa,node * destinatar);
void trimite_mesaj(struct sockaddr_in * adresa_nod,char * msg);
node cpf(uint32_t id);
char in_interval(uint32_t n1,uint32_t n2,uint32_t id);
void update_predecesor_of(node * partener,node * nod_nou);
void * functie(void * arg);
void creat_thread_i(int i);
void * thread_nou_pentru_citire_comenzi(void *);
void * tr1(void * arg);
void * tr2(void * arg);
void * tr3(void * arg);
node call_and_get_find_pred(node * partener,uint32_t id);
node call_and_get_find_succ(node * partener,uint32_t id);

static void * treat(void * arg);
void scrie(int sock,char * buff,int size)
{
	int k=0,f;
	while (1)
	{
		if (k==size) break;
		f=write(sock,buff+k,size-k);
		if (f<0)
		{
			perror("eroare la apelul write din functia scrie");
			exit(-1);
		} else
		k+=f;

	}
}

void update_succesor(node * x);//codul pentru apelul acesta va fi 0 pe 8 biti
node  get_predecesor_of(node * partener)//codul pentru apelul acesta va fi 1 pe 8 biti
{
	node p;
	//interoghez nodul partener despre predecesorul sau
	char msg[30];
	struct sockaddr_in part;
	completeaza_adresa_destinatar(&part,partener);
	msg[0]=0;
	trimite_mesaj(&part,msg);
	if (msg[0]==-2)
	 return call_and_get_find_pred(&n_prim,partener->eticheta);
	char c;
	despacheteaza(msg,&c,&p);
  return p;
}
void get_predecesor(uint32_t partener)
{
	//trimit intr-un mesaj pe socketul partener predecesorul meu
	char msg[30];
	impacheteaza(msg,0,predecesor.eticheta,predecesor.IP,predecesor.PORT);

	scrie(partener,msg,30);

}

node  get_closest_preceding_finger_of(node * partener,uint32_t id)//in loc de eticheta nodului partener pun id-ul al carui pred il caut
{
  partener->eticheta=id;
	char msg[30];
	impacheteaza(msg,1,partener->eticheta,partener->IP,partener->PORT);
	struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,partener);
	trimite_mesaj(&adr,msg);
	if (msg[0]==-2)
	{
		node k=call_and_get_find_pred(&n_prim,partener->eticheta);
	   return get_closest_preceding_finger_of(&k,id);
	}
	char c;
	node p;
	despacheteaza(msg,&c,&p);
	return p;
}

void get_closest_preceding_finger(uint32_t partener,uint32_t id)
{
	node p=cpf(id);
	char msg[30];
	impacheteaza(msg,1,p.eticheta,p.IP,p.PORT);
	scrie(partener,msg,30);

}

node  get_succesor_of(node * partener)//codul pentru apelul acesta va fi 1 pe 8 biti
{
	node p;
	//interoghez nodul partener despre succesorul sau
	char msg[30];
	struct sockaddr_in part;
	completeaza_adresa_destinatar(&part,partener);
	msg[0]=2;
	trimite_mesaj(&part,msg);
	if (msg[0]==-2)
	{/*fprintf(stderr, "puppppla\n" );*/  return call_and_get_find_succ(&n_prim,partener->eticheta);}
	char c;
	despacheteaza(msg,&c,&p);
  return p;
}
void get_succesor(uint32_t partener)
{
	//trimit intr-un mesaj pe socketul partener predecesorul meu
	char msg[30];
	impacheteaza(msg,2,succesor.eticheta,succesor.IP,succesor.PORT);

scrie(partener,msg,30);
}

node call_and_get_find_pred(node * partener,uint32_t id)
{
	struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,&n_prim);
	char msg[30];
	impacheteaza(msg,3,id,this.IP,this.PORT);
	trimite_mesaj(&adr,msg);
	if (msg[0]==-2)
	  return call_and_get_find_pred(&n_prim,id);
	char c;
	node p;
	despacheteaza(msg,&c,&p);
	return p;
}

node  find_pred(uint32_t id)
{
  node n=this;
	node succ=succesor;

	//fprintf(stderr,"succ meu %d eu %d pred meu %d id %d \n",succesor.eticheta,this.eticheta,predecesor.eticheta,id);

	if (this.eticheta==id)
	 return predecesor;

	while (in_interval(n.eticheta,succ.eticheta,id)==0)
    n=get_closest_preceding_finger_of(&n,id),succ=get_succesor_of(&n);
  return n;
}

void send_finded_pred(uint32_t partener,uint32_t id)
{

	node p=find_pred(id);
	char msg[30];
	impacheteaza(msg,3,p.eticheta,p.IP,p.PORT);
	scrie(partener,msg,30);
}

node call_and_get_find_succ(node * partener,uint32_t id)
{
	struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,partener);
	char msg[30];
	impacheteaza(msg,4,id,this.IP,this.PORT);
	trimite_mesaj(&adr,msg);
	if (msg[0]==-2)
	  return call_and_get_find_succ(&n_prim,id);
	char c;
	node p;
	despacheteaza(msg,&c,&p);
	return p;
}

node find_succ(uint32_t id)
{
	//fprintf(stderr, "caut succesorul lui %d din %d\n",id,this.eticheta );
  node n=find_pred(id);
	n=get_succesor_of(&n);
	return n;
}

void send_finded_succ(uint32_t partener,uint32_t id)
{
	node p=find_succ(id);
	char msg[30];
	impacheteaza(msg,4,p.eticheta,p.IP,p.PORT);
	scrie(partener,msg,30);

}

char in_interval(uint32_t n1,uint32_t n2,uint32_t id)
{
	if (n1<n2 && n1<id && id<=n2)
	   return 1;
	if (n1>=n2 && (n1<id || n2>=id))
	   return 1;
	 return 0;
}
char in_interval1(uint32_t n1,uint32_t n2,uint32_t id)
{
	if (n1<n2 && n1<id && id<n2)
	   return 1;
	if (n1>n2 && (n1<id || n2>id))
	   return 1;
	 return 0;
}
char in_interval2(uint32_t n1,uint32_t n2,uint32_t id)
{
	if (n1<n2 && n1<=id && id<n2)
	   return 1;
	if (n1>n2 && (n1<=id || n2>id))
	   return 1;
	 return 0;
}

void update_finger_table_of(node * p,node * s,uint32_t id)
{

	struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,p);
	char msg[30];
	impacheteaza(msg,6,s->eticheta,s->IP,s->PORT);
	memcpy(msg+23,&id,4);
	trimite_mesaj(&adr,msg);
}

void update_finger_table(node *s,uint32_t id)
{
  if (in_interval2(this.eticheta,finger_table[id].nod.eticheta,s->eticheta) && finger_table[id].nod.eticheta!=s->eticheta)
	 {
		 finger_table[id].nod=*s;
		 update_finger_table_of(&predecesor,s,id);
	 }
}

void update_others()
{
	int i;
	for (i=0;i<m;++i)
	 {
		 node p=find_pred((uint32_t)((this.eticheta-(1<<i)+(1<<m))&((1<<m)-1)));
		 //update_finger_table_of p
		 update_finger_table_of(&p,&this,i);
	 }
}

void impacheteaza(char * msg,char cod_operatie,uint32_t eticheta,uint32_t * IP,uint16_t PORT)
{
	memcpy(msg,&cod_operatie,1);
	memcpy(msg+1,&eticheta,4);
	memcpy(msg+5,IP,16);
	memcpy(msg+21,&PORT,2);

}

void despacheteaza(char * msg,char * code_operatie,node * t_node)
{
   memcpy(code_operatie,msg,1);
	 memcpy(&t_node->eticheta,msg+1,4);
	 memcpy(&t_node->IP,msg+5,16);
	 memcpy(&t_node->PORT,msg+21,2);
}

char * nodetochr(node * destinatar)
{
	char * p=(char *) malloc(30);
	char s[30];
	sprintf(s,"%d.",destinatar->IP[0]);
	p[0]=0;
	strcat(p,s);
	sprintf(s,"%d.",destinatar->IP[1]);
	strcat(p,s);
	sprintf(s,"%d.",destinatar->IP[2]);
	strcat(p,s);
  sprintf(s,"%d",destinatar->IP[3]);
	strcat(p,s);
  return p;
}

void completeaza_adresa_destinatar(struct sockaddr_in * adresa,node * destinatar)
{
	memset(adresa,0,sizeof(struct sockaddr_in));
	adresa->sin_family=AF_INET;
	char * p=nodetochr(destinatar);
	adresa->sin_addr.s_addr=inet_addr(p);
	free(p);
	adresa->sin_port=htons(destinatar->PORT);
}

void trimite_mesaj(struct sockaddr_in * adresa_nod,char * msg)
{
	//connect
	//send
	//receive
	uint32_t sock;

  if ((sock=socket(AF_INET,SOCK_STREAM,0))<0)
	  {
	  	perror("eroare la apelul socket");
	    exit(-1);
	  }

	if (connect(sock,(struct sockaddr *)adresa_nod,sizeof(struct sockaddr_in))<0)
	{
    if (msg[0]==9)
		{
			//a cazut nodul
			msg[0]=-1;
			return;
		} else
		  {
				perror("eroare la apelul connect din trimite_mesaj deci a cazut un nod");
			   msg[0]=-2;
				 return;
			}
	}
 scrie(sock,msg,30);
	 //acum astept raspuns
	 if (read(sock,msg,30)<0)
	 {
		 perror("eroare la apelul read din trimite_mesaj");
		 exit(-1);
	 }
	 close(sock);
}

//1 get_predecesor
//2 get_succesor
//0 update_succesor
//3 raspuns la get_predecesor
//4 raspuns la get_succesor


//daca primesc un cod pentru operatie atunci primesc si eticheta pentru a returna succesorul sa upredecesorul

void finger_table_init()
{
  finger_table[0].nod=succesor;
	//update_predecesor pentru succesor
  update_predecesor_of(&succesor,&this);
  update_succesor_of(&predecesor,&this);
	 int i;
	 for (i=1;i<m;++i)
	 {
		 if (in_interval2(this.eticheta,finger_table[i-1].nod.eticheta,finger_table[i].start))
		   finger_table[i].nod=finger_table[i-1].nod; else
			finger_table[i].nod=call_and_get_find_succ(&n_prim,finger_table[i].start);
	 }
}


void update_predecesor_of(node * partener,node * nod_nou)
{
	struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,partener);
	char msg[30];
	impacheteaza(msg,5,nod_nou->eticheta,nod_nou->IP,nod_nou->PORT);
	trimite_mesaj(&adr,msg);
}

void update_succesor_of(node * partener,node * nod_nou)
{
	struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,partener);
	char msg[30];
	impacheteaza(msg,7,nod_nou->eticheta,nod_nou->IP,nod_nou->PORT);
	trimite_mesaj(&adr,msg);
}


node cpf(uint32_t id)//closest preceding finger
{
	int i;
	for (i=m-1;i>=0;--i)
	if (in_interval1(this.eticheta,id,finger_table[i].nod.eticheta))
	  return finger_table[i].nod;
	return this;
}


int eticheta(char * s1,char * s2);

// trebuie sa fac un sistem de coduri pentru a recunoaste din mesajele primite ce apel de functii trebuie sa fac
//

void complete_this(char *s1, char * s2, node * this )
{
  this->eticheta=eticheta(s1,s2);
  this->PORT=atoi(s2);
  static int tmp,i,k;

  tmp=0;
  k=0;

  for (i=0;i<strlen(s1);++i)
  if (s1[i]=='.')
  {
  	this->IP[k++]=tmp;
  	tmp=0;
  } else
    tmp=tmp*10+s1[i]-48;

   this->IP[k]=tmp;

}


int eticheta(char * s1,char * s2)
{
	static char s[30];
	static char hash[30];
		s[0]=0;
		strcat(s,s1);
		strcat(s,":");
		strcat(s,s2);

		SHA1(s,strlen(s),hash);
        //utilizez primii 8 biti din hash

		return (hash[0]+256)&(255);
}
int eticheta1(char * s1)
{
	static char s[30];
	static char hash[30];
		s[0]=0;
		strcat(s,s1);

		SHA1(s,strlen(s),hash);
        //utilizez primii 8 biti din hash

		return (hash[0]+256)&(255);
}



void initializare(char * s1,char * s2)
{
   memset(&adresa_nod,0,sizeof(adresa_nod));

	adresa_nod.sin_family=AF_INET;
	adresa_nod.sin_addr.s_addr=inet_addr(s1);
	adresa_nod.sin_port=htons(atoi(s2));

    if ((sock=socket(AF_INET,SOCK_STREAM,0))<0)
    {
    	perror("eroare la apelul socket");
    	exit(-1);
    }

    if (setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&enb,sizeof(uint32_t))==-1)
    {
    	perror("eroare la apelul setsockopt");
    	exit(-1);
    }

    if (bind(sock,(struct sockaddr *) &adresa_nod,sizeof(adresa_nod))<0)
    {
    	perror("eroare la apelul bind");
    	exit(-1);
    }

		if (listen(sock,10000)<0)
		{
			perror("eroare la apelul listen");
			exit(-1);
		}
}

//*******************************************************************//
void join(node * n_prim)
{
	//predecesor=nil se echivaleaza cu - la eticheta
	predecesor.eticheta=-1;
	succesor=call_and_get_find_succ(n_prim,this.eticheta);
}

void stabilizeaza()
{
	node x=get_predecesor_of(&succesor);
//fprintf(stderr, "apel din stabilizare sunt in %d succesor este %d si predecesorul succ este %d \n",this.eticheta,succesor.eticheta,x.eticheta );

	if (in_interval1(this.eticheta,succesor.eticheta,x.eticheta))
	  succesor=x;
	//notify

	finger_table[0].nod=succesor;

	notifica_pe(&succesor,&this);
}

void notifica_pe(node * n_prim,node * nod)
{
   //notific pe n_prim cu parametrul nod
	 struct sockaddr_in adr;
	 completeaza_adresa_destinatar(&adr,n_prim);
	 char msg[30];
	 impacheteaza(msg,8,nod->eticheta,nod->IP,nod->PORT);
	 trimite_mesaj(&adr,msg);
}

void notifica(node * n_prim)
{
//	fprintf(stderr, "\n\n\npredecesorul actualizat este %d din this= %d %d\n\n\n",predecesor.eticheta,this.eticheta,n_prim->eticheta );
	if (predecesor.eticheta==-1 || in_interval1(predecesor.eticheta,this.eticheta,n_prim->eticheta))
	predecesor=*n_prim;

}

void fixeaza_finger()
{
  if (next_finger>=m)
	next_finger=0;
//	fprintf(stderr, "finger vechi %d id %d ",finger_table[next_finger].nod.eticheta,next_finger );
	finger_table[next_finger].nod=call_and_get_find_succ(&this,finger_table[next_finger].start);
//	fprintf(stderr, "finger nou %d \n",finger_table[next_finger].nod.eticheta );
	next_finger++;
}

void verifica_predecesor()
{
	struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,&predecesor);
	char msg[30];
	msg[0]=9;
	trimite_mesaj(&adr,msg);
	if (msg[0]==-1)
	 {
		 //a cazut Predecesorul

		 node p=find_pred(predecesor.eticheta);
		 //fprintf(stderr, "a cazut nodul %d cu pred %d  cu succ %d \n",predecesor.eticheta,p.eticheta,succesor.eticheta );
		 update_succesor_of(&p,&this);
		 predecesor=p;//predecesor.eticheta=-1;
	 }
}


void transmite_cheia_valoare(node * n,uint32_t k,char * s,char op)
{
	char msg[30];
	msg[0]=op;
	memcpy(msg+1,&k,4);
	memcpy(msg+5,s,strlen(s));
	msg[strlen(s)+5]=0;
	struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,n);
	trimite_mesaj(&adr,msg);
}

void baga_la_valori_proprii(uint32_t k,char * s)
{
  chei_proprii[nr_valori_proprii]=k;
	strcpy(valori_proprii[nr_valori_proprii],s);
	nr_valori_proprii++;
}

void baga_la_valori_straine(uint32_t k,char * s)
{
  chei_straine[nr_valori_straine]=k;
	strcpy(valori_straine[nr_valori_straine],s);
	nr_valori_straine++;
}

void restabilesc_tabelul_cu_chei()
{
	uint32_t aux1[100];
	char aux2[100][100];
	int i,k=0;
	for (i=0;i<nr_valori_proprii;++i)
	 if (chei_proprii[i]!=-1)
	   {
			 aux1[k]=chei_proprii[i];
			 strcpy(aux2[k],valori_proprii[i]);
			 k++;
		 }
	nr_valori_proprii=k;
	for (i=0;i<k;++i)
	 {
		 chei_proprii[i]=aux1[i];
		 strcpy(valori_proprii[i],aux2[i]);
	 }
}

void get_value_from(node * n,uint32_t k,char * s)
{
  struct sockaddr_in adr;
	completeaza_adresa_destinatar(&adr,n);
	char msg[30];
  msg[0]=12;
	memcpy(msg+1,&k,4);
	trimite_mesaj(&adr,msg);
  strcpy(s,msg);
}

void get_value(uint32_t k,char * s)
{
	int i;
	for (i=0;i<nr_valori_proprii;++i)
	 if (chei_proprii[i]==k)
	 {
	   strcpy(s,valori_proprii[i]);
		 return;
	 }
	 strcpy(s,"nu detin aceasta cheie");
}


//trebuie sa ma conectez undeva
//sa trimit operatia si sa astept raspunsul
int main(int argc, char  *argv[])
{
	if (argc<3)
		exit(-1);
	//argv[1] este IP
	//argv[2] este portul
	//initial incerc sa ma conectez la 127.0.0.1 2040
//mai intai citesc perechile cheie valoare  si le mapez
  fprintf(stderr, "introduceti numarul de perechi care va fi citit\n" );
	int ll,k;
	scanf("%d",&ll);
 for (k=0;k<ll;++k)
		scanf("%s%s",s[0][k],s[1][k]);


  complete_this(argv[1],argv[2],&this);
  initializare(argv[1],argv[2]);
  complete_this("127.0.0.1","2040",&n_prim);

	fprintf(stderr,"Eticheta mea este %d\n",this.eticheta);

  struct sockaddr_in adresa_nod_partener;

	int i;
   for  (i=0;i<10;++i)
 	 creat_thread_i(i);

  if (strcmp(argv[2],"2040")==0)
  {
  	//primul nid din retea
    succesor=this;
    predecesor=this;
		int i;
		for (i=0;i<m;++i)
		 {
			 finger_table[i].start=(this.eticheta+(1<<i))&((1<<m)-1);// n+2^i modulo 2^m (aici iau ultimii m biti din suma)
			 finger_table[i].nod=this;
		 }
    printf("M-am conectat \nSuccesorul meu este %d \nPredecesorul meu este %d \n",succesor.eticheta,predecesor.eticheta);
    fflush(stdout);
  } else
  {
  	//interoghez nodul cu ip 127.0.0.1 si portul 2040 despre succesorul meu

    int i;
		for (i=0;i<m;++i)
		  finger_table[i].start=(this.eticheta+(1<<i))%((1<<m));
    join(&n_prim);
    predecesor=call_and_get_find_pred(&n_prim,this.eticheta);
    succesor=call_and_get_find_succ(&n_prim,this.eticheta);
		printf("M-am conectat \nSuccesorul meu este %d \nPredecesorul meu este %d \n",succesor.eticheta,predecesor.eticheta);
    fflush(stdout);
		finger_table_init();
   }

if (strcmp(argv[2],"2040")!=0)
 update_others();

 int j;
 for (j=0;j<m;++j)
 fprintf(stderr,"finger %d %d \n",j,finger_table[j].nod.eticheta);

 for (j=0;j<ll;++j)
	 {
		  uint32_t k_eticheta=eticheta1(s[0][j]);
		  node c=call_and_get_find_pred(&this,k_eticheta);
                 // fprintf(stderr,"pred este %d \n",c.eticheta);
			transmite_cheia_valoare(&c,k_eticheta,s[1][j],10);
	 }

	 if (strcmp(argv[2],"2040")!=0)
	 {
		 struct sockaddr_in adr;
		 completeaza_adresa_destinatar(&adr,&predecesor);
		 char msg[30];
		 msg[0]=11;
		 trimite_mesaj(&adr,msg);
	 }

 pthread_create(&t,NULL,&thread_nou_pentru_citire_comenzi,NULL);
 pthread_create(&t1,NULL,&tr1,NULL);
 pthread_create(&t2,NULL,&tr2,NULL);
 pthread_create(&t3,NULL,&tr3,NULL);
 while (1) ;
	return 0;
}

void creat_thread_i(int i)
{
	pthread_create(&thread[i].id,NULL,&functie,(void *) i);
}


void * functie(void * arg)
{
	struct sockaddr_in adr;
	uint32_t len=sizeof(adr);

	while (1)
	{
		//astept sa citesc un mesaj de 23 de bytes cu o interogare
			//trebuie sa tratez concurent fiecare cerere
			char msg[30];
			uint32_t nod_partener;

			//fprintf(stderr,"succesorul meu este %d \n predecesorul meu este %d \n",succesor.eticheta,predecesor.eticheta);
			pthread_mutex_lock(&lock);
				if ((nod_partener=accept(sock,(struct sockaddr *)&adr,&len))<0)
				{
					perror("eroare la apelul accept");
					exit(-1);
				}
	      thread[(int) arg].client=nod_partener;
	     pthread_mutex_unlock(&lock);

			 if (recv(thread[(int) arg].client,thread[(int) arg].msg,30,0)<0)
			 {
				 perror("eroare la apelul recv din zona de accept");
				 continue;
			 }

	     despacheteaza(thread[(int) arg].msg,&thread[(int) arg].op,&thread[(int)arg].nod);
  if (thread[(int) arg].op==-1)//nu stiu ce se intampla
	{

	} else
	if (thread[(int) arg].op==0) //get_predecesor
	 {
			get_predecesor(thread[(int) arg].client);
	 } else
	 if (thread[(int) arg].op==1)//get_closest_preceding_finger
		{
			//aici trebuie sa tin cont ca id pentru care caut closest precedin finger este eticheta din nodul primit
			get_closest_preceding_finger(thread[(int) arg].client,thread[(int) arg].nod.eticheta);
		} else
		if (thread[(int) arg].op==2)//get_succesor
		 {
			 get_succesor(thread[(int) arg].client);
		 } else
		 if (thread[(int) arg].op==3)//find_pred
		 {
			 //eticheta la nodul interogare corespunde id-ului predecesorul caruia trebuei de returnata
			 send_finded_pred(thread[(int) arg].client,thread[(int) arg].nod.eticheta);
		 } else
		 if (thread[(int) arg].op==4)//find_succ
		 {
			 //eticheta la nodul interogare corespunde id-ului succesorul caruia trebuei de returnata
			 send_finded_succ(thread[(int) arg].client,thread[(int) arg].nod.eticheta);
		 } else
		 if (thread[(int) arg].op==5)
		 {
				predecesor=thread[(int) arg].nod;
				scrie(thread[(int) arg].client,thread[(int) arg].msg,30);
		 }else
		 if (thread[(int) arg].op==6)
		 {
			 memcpy(&thread[(int) arg].k,thread[(int) arg].msg+23,4);
			 update_finger_table(&thread[(int) arg].nod,thread[(int) arg].k);
			scrie(thread[(int) arg].client,thread[(int) arg].msg,30);
		 } else
		 if (thread[(int) arg].op==7)
		 {
				succesor=thread[(int) arg].nod;
				finger_table[0].nod=succesor;
				scrie(thread[(int) arg].client,thread[(int) arg].msg,30);
		 } else
		 if (thread[(int) arg].op==8)
		 {
			 notifica(&thread[(int) arg].nod);
			 scrie(thread[(int) arg].client,thread[(int) arg].msg,30);
		 } else
		 if (thread[(int) arg].op==9)
		  {
				scrie(thread[(int) arg].client,thread[(int) arg].msg,30);
			} else
			if (thread[(int) arg].op==10)
 		  {
				uint32_t k;
				memcpy(&k,thread[(int) arg].msg+1,4);
				baga_la_valori_proprii(k,thread[(int) arg].msg+5);
 				scrie(thread[(int) arg].client,thread[(int) arg].msg,30);
 			} else
			if (thread[(int) arg].op==11)
 		  {
				int i;
				for (i=0;i<nr_valori_proprii;++i)
{
                       //fprintf(stderr,"pred %d this %d chei %d \n",predecesor.eticheta,this.eticheta,chei_proprii[i]);
				 if (in_interval(this.eticheta,succesor.eticheta,chei_proprii[i])==0)
         {
					 //fprintf(stderr, "trimit cheia %d lui %d\n",chei_proprii[i],succesor.eticheta );
					 transmite_cheia_valoare(&succesor,chei_proprii[i],valori_proprii[i],10);
					 chei_proprii[i]=-1;
				 }
}
				restabilesc_tabelul_cu_chei();
 			} else
			if (thread[(int) arg].op==12)
			{
				uint32_t k;
				memcpy(&k,thread[(int) arg].msg+1,4);
				get_value(k,thread[(int) arg].msg);
				//scrim rezult
				scrie(thread[(int) arg].client,thread[(int) arg].msg,30);
			}else
			if (thread[(int) arg].op==13)
			{
				int i;
				uint32_t k;
				memcpy(&k,thread[(int) arg].msg+1,4);
				for (i=0;i<nr_valori_proprii;++i)
				 if (chei_proprii[i]==k)
				   {
						 strcpy(valori_proprii[i],thread[(int) arg].msg+5);
						 break;
					 }
				//scrim rezult
				scrie(thread[(int) arg].client,thread[(int) arg].msg,30);
			}
		 close(thread[(int) arg].client);
	 }
}

void * thread_nou_pentru_citire_comenzi(void * arg)
{
	while (1)
	{
		char comanda[30];
		scanf("%s",comanda);
		if (strcmp(comanda,"show_finger_table")==0)
		{
			fprintf(stderr, "\n\nfinger table : \n");
       int k;
			 for (k=0;k<m;++k)
			  fprintf(stderr,"finger_table[%d]=%d\n",k,finger_table[k].nod.eticheta);
				fprintf(stderr, "\n\n");
		} else
		if (strcmp(comanda,"find_pred")==0)
		 {
			 char nr[30];
			 scanf("%s",nr);
			 int k=atoi(nr);
			 fprintf(stderr, "\n\n%d\n\n",find_pred(k).eticheta);
		 } else
		 if (strcmp(comanda,"find_succ")==0)
		 {
			 char nr[30];
				scanf("%s",nr);
				int k=atoi(nr);
				fprintf(stderr, "\n\n%d\n\n",find_succ(k).eticheta);
		 } else
		 if (strcmp(comanda,"show_values")==0)
		  {
				int i;
				for (i=0;i<nr_valori_proprii;++i)
				 fprintf(stderr, "%d %s\n",chei_proprii[i],valori_proprii[i]);
if (nr_valori_proprii==0) fprintf(stderr,"nu detin nici o cheie\n\n");
			} else
			if (strcmp(comanda,"get_pred")==0)
			{
				fprintf(stderr, "\n\n%d\n\n",find_pred(this.eticheta).eticheta);
			}else
			if (strcmp(comanda,"get_succ")==0)
			{
				fprintf(stderr, "\n\n%d\n\n",find_succ(this.eticheta+1).eticheta);
			} else
			if (strcmp(comanda,"get_value_for_key")==0)
			{
				char sl[30];
				scanf("%s",sl);
				uint32_t k=eticheta1(sl);
				//fprintf(stderr,"caut cheia %d \n",k);
				node c=find_pred(k);
				get_value_from(&c,k,sl);
				fprintf(stderr, "%s\n",sl );
			} else
			if (strcmp(comanda,"change")==0)
			{
				char c1[30],c2[30];
				scanf("%s%s",c1,c2);
        uint32_t k=eticheta1(c1);
				c1[0]=13;
				memcpy(c1+1,&k,4);
				strcpy(c1+5,c2);
				struct sockaddr_in adr;
				node c=find_pred(k);
				completeaza_adresa_destinatar(&adr,&c);
				trimite_mesaj(&adr,c1);
			}
	}
}

void * tr1(void * arg)
{
	while (1)
	{
		stabilizeaza();
		sleep(1);
	}
}
void * tr2(void * arg)
{
	while (1)
	{
		verifica_predecesor();
		sleep(1);
	}
}

void * tr3(void * arg)
{
	while (1)
	{
		sleep(1);
		fixeaza_finger();

	}
}
