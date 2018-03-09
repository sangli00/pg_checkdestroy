#Install

make
make install

you can modify postgres.conf 
shared_preload_libraries='pg_checkdestroy'


#Parameter

pg_checkdestroy.work default on
This is PGC_USERSET guc parameter,you can set in session.


WANING:This code is not test full case,maybe some case is cannot work,I don't advice use online system. 
