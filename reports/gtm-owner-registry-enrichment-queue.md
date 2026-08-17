# GTM Owner Registry Enrichment Queue

Generated: 2026-07-04T11:32:41.808Z

## Summary

- Total queue items: **30**
- Priority 1 (Tier A): **30**

### By verification status

- needs_registry: 25
- needs_manual_country: 4
- needs_verification: 1

### By primary country

- Mexico: 12
- Brazil: 7
- Cuba: 2
- Argentina: 1
- Colombia: 1
- Dominican Republic: 3
- Chile: 2
- Peru: 1
- El Salvador: 1

## Work items (top 25)

### Fibra Hotel Mexico (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 74
- **Primary path:** corporate_web_first
- **Corporate site:** https://fibrahotel.mx
- **Entity type:** public_reit
- **Recommended contact:** Eduardo López García — Chief Executive Officer
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Fibra Hotel Mexico
- **Sample properties:**
  - One Hoteles Guadalajara Tapatio — Guadalajara — Mexico
  - Fiesta Inn Periférico Sur — Mexico City — Mexico
  - Fiesta Inn Ciudad Juarez — Ciudad Juárez — Mexico
  - One Durango — Durango — Mexico
  - AC Hotels by Marriott Queretaro Antea — Queretaro — Mexico
- **Hints:**
  - CoStar True Owner: Fibra Hotel Mexico
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### Nacional Inn Hotéis (A, P1)

- **Status:** needs_registry
- **Country:** Brazil
- **Registry:** Brazil — Receita Federal CNPJ + Cadastur
- **Bridge:** direct_entity
- **CALA properties:** 57
- **Commercial registry:** https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/Cnpjreva_Solicitacao.asp
- **Tourism registry:** https://cadastur.turismo.gov.br/
- **Entity search:** Nacional Inn Hotéis
- **Sample properties:**
  - Dan Inn Uberaba — Uberaba — Brazil
  - Dan Inn Barretos — Brazil
  - Dan Inn Cambuí Campinas — Campinas — Brazil
  - Golden Park Campinas Viracopos — Campinas — Brazil
  - Dan Inn Express Foz do Iguaçu — Foz do Iguaçu — Brazil
- **Hints:**
  - CoStar True Owner: Nacional Inn Hotéis
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - CNPJ lookup returns razão social, status, address; QSA/admins vary by state junta.
  - Cadastur for tourism operator registration.

### Grupo de Turismo Gaviota (A, P1)

- **Status:** needs_manual_country
- **Country:** Cuba
- **Registry:** —
- **Bridge:** direct_entity
- **CALA properties:** 48
- **Entity search:** Grupo de Turismo Gaviota
- **Sample properties:**
  - Villa Maguana — Cuba
  - Hotel Playa Vista Azul Varadero — Varadero — Cuba
  - Hotel Playa Pesquero Resort Suite and Spa — Cuba
  - Hotel El Bosque — Cuba
  - Hotel Los Helechos — Trinidad — Cuba
- **Hints:**
  - CoStar True Owner: Grupo de Turismo Gaviota
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).

### ICH Administracao de Hoteis S.A. (A, P1)

- **Status:** needs_registry
- **Country:** Brazil
- **Registry:** Brazil — Receita Federal CNPJ + Cadastur
- **Bridge:** direct_entity
- **CALA properties:** 42
- **Commercial registry:** https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/Cnpjreva_Solicitacao.asp
- **Tourism registry:** https://cadastur.turismo.gov.br/
- **Entity search:** ICH Administracao de Hoteis S.A.
- **Sample properties:**
  - Intercity Porto Alegre Praia de Belas — Porto Alegre — Brazil
  - Intercity Porto Maravilha Hotel — Rio de Janeiro — Brazil
  - InterCity Interative Jardins — São Paulo — Brazil
  - Hotel Intercity Florianópolis — Florianópolis — Brazil
  - InterCity Bauru — Bauru — Brazil
- **Hints:**
  - CoStar True Owner: ICH Administracao de Hoteis S.A.
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - CNPJ lookup returns razão social, status, address; QSA/admins vary by state junta.
  - Cadastur for tourism operator registration.

### Howard Johnson Argentina (A, P1)

- **Status:** needs_manual_country
- **Country:** Argentina
- **Registry:** —
- **Bridge:** direct_entity
- **CALA properties:** 34
- **Entity search:** Howard Johnson Argentina
- **Sample properties:**
  - Howard Johnson by Wyndham Rio Ceballos — Argentina
  - Howard Johnson by Wyndham Rio Cuarto Casino — Río Cuarto — Argentina
  - Howard Johnson by Wyndham Ciudad del Este — Ciudad Del Este — Paraguay
  - Howard Johnson by Wyndham San Pedro Resort & Marina — Argentina
  - Howard Johnson by Wyndham Formosa Casino — Formosa — Argentina
- **Hints:**
  - CoStar True Owner: Howard Johnson Argentina
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).

### Fibra Inn (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 28
- **Primary path:** corporate_web_first
- **Corporate site:** https://fibrainn.mx
- **Entity type:** public_reit
- **Recommended contact:** Sergio Martinez Richo — Director of Investor Relations & ESG — ir@fibrainn.mx
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Fibra Inn
- **Sample properties:**
  - Marriott Puebla Hotel Meson del Angel — Puebla — Mexico
  - Casa Grande Chihuahua — Chihuahua — Mexico
  - Crowne Plaza Monterrey Aeropuerto — Apodaca — Mexico
  - Hampton Inn by Hilton Reynosa/Zona Industrial — Reynosa — Mexico
  - Hampton by Hilton Saltillo — Saltillo — Mexico
- **Hints:**
  - CoStar True Owner: Fibra Inn
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### Real Hotels & Resorts (A, P1)

- **Status:** needs_registry
- **Country:** Colombia
- **Registry:** Colombia — RUES + RNT
- **Bridge:** direct_entity
- **CALA properties:** 22
- **Commercial registry:** https://www.rues.org.co/
- **Tourism registry:** https://www.rues.org.co/
- **Entity search:** Real Hotels & Resorts
- **Sample properties:**
  - JW Marriott Hotel Bogota — Bogota — Colombia
  - Courtyard by Marriott Panama Metromall — San Miguelito — Panama
  - InterContinental Real Guatemala — Guatemala City — Guatemala
  - InterContinental Tegucigalpa at Multiplaza Mall — Tegucigalpa — Honduras
  - InterContinental Real Santo Domingo — Santo Domingo — Dominican Republic
- **Hints:**
  - CoStar True Owner: Real Hotels & Resorts
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - Free RUES search by NIT or razón social returns representante legal.
  - Tourism (RNT) consultable within RUES portal by NIT or RNT number.

### Servicios Corporativos Piñero S.L. (A, P1)

- **Status:** needs_registry
- **Country:** Dominican Republic
- **Registry:** Dominican Republic — Registro Mercantil + DGII RNC
- **Bridge:** direct_entity
- **CALA properties:** 17
- **Commercial registry:** https://app.registromercantil.do/
- **Entity search:** Servicios Corporativos Piñero S.L.
- **Sample properties:**
  - Bahia Principe Explore Turquesa — Punta Cana — Dominican Republic
  - Bahia Principe Explore Akumal — Akumal — Mexico
  - Bahia Principe Grand El Portillo — Dominican Republic
  - Bahia Principe Grand Tequila — Tulum — Mexico
  - Bahia Principe Escape Ambar — Punta Cana — Dominican Republic
- **Hints:**
  - CoStar True Owner: Servicios Corporativos Piñero S.L.
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - Account required on app.registromercantil.do for Santo Domingo / DN searches.
  - Returns razón social, RNC, legal representative, registered address.
  - Provincial chambers for assets outside DN.

### Bourbon Hotels & Resorts (A, P1)

- **Status:** needs_registry
- **Country:** Brazil
- **Registry:** Brazil — Receita Federal CNPJ + Cadastur
- **Bridge:** direct_entity
- **CALA properties:** 13
- **Commercial registry:** https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/Cnpjreva_Solicitacao.asp
- **Tourism registry:** https://cadastur.turismo.gov.br/
- **Entity search:** Bourbon Hotels & Resorts
- **Sample properties:**
  - Bourbon Curitiba Convention Hotel — Curitiba — Brazil
  - Bourbon Barra Premium Residence — Rio de Janeiro — Brazil
  - Bourbon Joinville Convention Hotel — Joinville — Brazil
  - Bourbon Fortaleza Hotel — Fortaleza — Brazil
  - Bourbon Resort Atibaia — Brazil
- **Hints:**
  - CoStar True Owner: Bourbon Hotels & Resorts
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - CNPJ lookup returns razão social, status, address; QSA/admins vary by state junta.
  - Cadastur for tourism operator registration.

### Essendi (A, P1)

- **Status:** needs_registry
- **Country:** Chile
- **Registry:** Chile — Registro de Empresas
- **Bridge:** direct_entity
- **CALA properties:** 13
- **Commercial registry:** https://www.registrodeempresasysociedades.cl/
- **Entity search:** Essendi
- **Sample properties:**
  - Ibis Cancun Centro — Cancun — Mexico
  - ibis Santiago La Condes Manquehue — Santiago — Chile
  - Novotel Cusco — Cusco — Peru
  - ibis Santiago Providencia — Santiago — Chile
  - Ibis Antofagasta — Antofagasta — Chile
- **Hints:**
  - CoStar True Owner: Essendi
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - Free search by RUT or razón social for representatives.

### Irawadi Corp S.A. (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 13
- **Primary path:** corporate_web_first
- **Corporate site:** https://rcdhotels.com
- **Entity type:** foreign_hq
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Irawadi Corp S.A.
- **Sample properties:**
  - Hard Rock Hotel Los Cabos — Cabo San Lucas — Mexico
  - Hard Rock Hotel & Casino Punta Cana — Dominican Republic
  - Hard Rock Hotel Riviera Maya — Playa Del Carmen — Mexico
  - UNICO 20°105° Hotel Riviera Nayarit — Nuevo Vallarta — Mexico
  - Residence Inn Playa Del Carmen — Playa Del Carmen — Mexico
- **Hints:**
  - CoStar True Owner: Irawadi Corp S.A.
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### Habaguanex Tourist Company (A, P1)

- **Status:** needs_manual_country
- **Country:** Cuba
- **Registry:** —
- **Bridge:** direct_entity
- **CALA properties:** 12
- **Entity search:** Habaguanex Tourist Company
- **Sample properties:**
  - Hotel Marques De Prado Ameno — Havana — Cuba
  - Hotel Florida — Havana — Cuba
  - Hotel Ambos Mundos — Havana — Cuba
  - Hotel del Tejadillo — Havana — Cuba
  - Hotel Palacio del Marques de San Felipe y Santiago de Bejucal — Havana — Cuba
- **Hints:**
  - CoStar True Owner: Habaguanex Tourist Company
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).

### Park Royal Hotels & Resorts (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 12
- **Primary path:** corporate_web_first
- **Corporate site:** https://parkroyalhotels.com
- **Entity type:** private_operator
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Park Royal Hotels & Resorts
- **Sample properties:**
  - Park Royal Homestay Los Cabos — San Jose Del Cabo — Mexico
  - Club Regina Los Cabos managed by Accor — San Jose Del Cabo — Mexico
  - Park Royal Huatulco managed by Accor — Huatulco — Mexico
  - Park Royal Homestay Los Tules — Puerto Vallarta — Mexico
  - Park Royal Mazatlan managed by AccorHotels — Mazatlan — Mexico
- **Hints:**
  - CoStar True Owner: Park Royal Hotels & Resorts
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### BTG Pactual (A, P1)

- **Status:** needs_registry
- **Country:** Brazil
- **Registry:** Brazil — Receita Federal CNPJ + Cadastur
- **Bridge:** direct_entity
- **CALA properties:** 11
- **Commercial registry:** https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/Cnpjreva_Solicitacao.asp
- **Tourism registry:** https://cadastur.turismo.gov.br/
- **Entity search:** BTG Pactual
- **Sample properties:**
  - ibis Sao Paulo Morumbi — São Paulo — Brazil
  - Ibis Sorocaba — Sorocaba — Brazil
  - Fairmont Rio de Janeiro Copacabana — Rio de Janeiro — Brazil
  - ibis budget Porto Alegre — Porto Alegre — Brazil
  - Ibis Piracicaba — Piracicaba — Brazil
- **Hints:**
  - CoStar True Owner: BTG Pactual
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - CNPJ lookup returns razão social, status, address; QSA/admins vary by state junta.
  - Cadastur for tourism operator registration.

### Catalonia Hotels & Resorts (A, P1)

- **Status:** needs_registry
- **Country:** Dominican Republic
- **Registry:** Dominican Republic — Registro Mercantil + DGII RNC
- **Bridge:** direct_entity
- **CALA properties:** 11
- **Commercial registry:** https://app.registromercantil.do/
- **Entity search:** Catalonia Hotels & Resorts
- **Sample properties:**
  - Catalonia Costa Mujeres — Isla Mujeres — Mexico
  - Catalonia Royal Tulum Resort — Mexico
  - Catalonia Playa Maroma — Playa Del Carmen — Mexico
  - Catalonia Royal Bavaro — Punta Cana — Dominican Republic
  - Catalonia Santo Domingo — Santo Domingo — Dominican Republic
- **Hints:**
  - CoStar True Owner: Catalonia Hotels & Resorts
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - Account required on app.registromercantil.do for Santo Domingo / DN searches.
  - Returns razón social, RNC, legal representative, registered address.
  - Provincial chambers for assets outside DN.

### Estancias Extendidas (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 11
- **Primary path:** corporate_web_first
- **Corporate site:** https://estanciashoteles.com
- **Entity type:** private_operator
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Estancias Extendidas
- **Sample properties:**
  - Extended Suites Monterrey Aeropuerto — Apodaca — Mexico
  - Extended Suites Querétaro Juriquilla — Queretaro — Mexico
  - Extended Suites Cancún Cumbres — Cancun — Mexico
  - Extended Suites Saltillo Galerías — Saltillo — Mexico
  - Extended Suites Mexicali Cataviña — Mexicali — Mexico
- **Hints:**
  - CoStar True Owner: Estancias Extendidas
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### Grupo Brisas (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 11
- **Primary path:** corporate_web_first
- **Corporate site:** https://brisas.com.mx
- **Entity type:** private_operator
- **Recommended contact:** Antonio Cosío Pando — Director General / CEO — info@brisas.com.mx
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Grupo Brisas
- **Sample properties:**
  - Galeria Plaza Veracruz — Boca del Río — Mexico
  - Las Brisas Huatulco — Huatulco — Mexico
  - Galeria Plaza San Jeronimo — Mexico City — Mexico
  - Las Hadas By Brisas — Manzanillo — Mexico
  - Las Brisas Acapulco — Acapulco — Mexico
- **Hints:**
  - CoStar True Owner: Grupo Brisas
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### A3 Property Investment (A, P1)

- **Status:** needs_registry
- **Country:** Chile
- **Registry:** Chile — Registro de Empresas
- **Bridge:** direct_entity
- **CALA properties:** 9
- **Commercial registry:** https://www.registrodeempresasysociedades.cl/
- **Entity search:** A3 Property Investment
- **Sample properties:**
  - Pullman Lima San Isidro — San Isidro — Peru
  - Pullman Lima Miraflores — Lima — Peru
  - Pullman Santiago Vitacura — Santiago — Chile
  - Manto Hotel Lima MGallery Collection — Lima — Peru
  - Novotel Santiago Las Condes — Santiago — Chile
- **Hints:**
  - CoStar True Owner: A3 Property Investment
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - Free search by RUT or razón social for representatives.

### JHSF (A, P1)

- **Status:** needs_registry
- **Country:** Brazil
- **Registry:** Brazil — Receita Federal CNPJ + Cadastur
- **Bridge:** direct_entity
- **CALA properties:** 9
- **Commercial registry:** https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/Cnpjreva_Solicitacao.asp
- **Tourism registry:** https://cadastur.turismo.gov.br/
- **Entity search:** JHSF
- **Sample properties:**
  - Fasano São Paulo — São Paulo — Brazil
  - Fasano Angra Dos Reis — Angra dos Reis — Brazil
  - Fasano Punta Del Este — Punta Del Este — Uruguay
  - Boa Vista Surf Lodge — Brazil
  - Fasano Trancoso — Brazil
- **Hints:**
  - CoStar True Owner: JHSF
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - CNPJ lookup returns razão social, status, address; QSA/admins vary by state junta.
  - Cadastur for tourism operator registration.

### Pueblo Bonito Hotels and Resorts (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 9
- **Primary path:** corporate_web_first
- **Corporate site:** https://pueblobonito.com
- **Entity type:** private_operator
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Pueblo Bonito Hotels and Resorts
- **Sample properties:**
  - Pueblo Bonito Vantage San Miguel de Allende — San Miguel de Allende — Mexico
  - Pueblo Bonito Rosé Resort & Spa — Cabo San Lucas — Mexico
  - Pueblo Bonito Mazatlán Beach Resort — Mazatlan — Mexico
  - Pueblo Bonito Pacifica Golf & Spa Resort — Cabo San Lucas — Mexico
  - Pueblo Bonito Los Cabos Beach Resort — Cabo San Lucas — Mexico
- **Hints:**
  - CoStar True Owner: Pueblo Bonito Hotels and Resorts
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### Urbanova (A, P1)

- **Status:** needs_registry
- **Country:** Peru
- **Registry:** Peru — SUNARP + MINCETUR
- **Bridge:** direct_entity
- **CALA properties:** 9
- **Commercial registry:** https://www.sunarp.gob.pe/
- **Entity search:** Urbanova
- **Sample properties:**
  - Tambo del Inka, a Luxury Collection Resort & Spa, Valle Sagrado — Urubamba — Peru
  - JW Marriott Hotel Lima — Lima — Peru
  - The Westin Lima Hotel & Convention Center — San Isidro — Peru
  - Hotel Paracas, a Luxury Collection Resort, Paracas — Peru
  - JW Marriott El Convento Cusco — Cusco — Peru
- **Hints:**
  - CoStar True Owner: Urbanova
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - SUNARP for legal entity; MINCETUR for tourism registry.

### Eurostars Hotel Company S.L. (A, P1)

- **Status:** needs_verification
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 8
- **Primary path:** corporate_web_first
- **Corporate site:** https://eurostarshotels.com
- **Entity type:** foreign_hq
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Eurostars Hotel Company S.L.
- **Sample properties:**
  - Eurostars Zona Rosa Suites — Mexico City — Mexico
  - Exe Colon — Buenos Aires — Argentina
  - Exe Suites San Marino — Mexico City — Mexico
  - Crisol Mundial — Buenos Aires — Argentina
  - Exe Suites Reforma — Mexico City — Mexico
- **Hints:**
  - CoStar True Owner: Eurostars Hotel Company S.L.
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### Grupo Diestra (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 8
- **Primary path:** corporate_web_first
- **Corporate site:** https://grupodiestra.com
- **Entity type:** private_operator
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Grupo Diestra
- **Sample properties:**
  - Tijuana Marriott Hotel — Tijuana — Mexico
  - Emporio Hotel Mazatlan — Mazatlan — Mexico
  - JW Marriott Los Cabos Beach Resort & Spa — San Jose Del Cabo — Mexico
  - Villahermosa Marriott Hotel — Villahermosa — Mexico
  - Samba Vallarta by Emporio — Nuevo Vallarta — Mexico
- **Hints:**
  - CoStar True Owner: Grupo Diestra
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### Hoteles MX (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 8
- **Primary path:** corporate_web_first
- **Corporate site:** https://hotelesmx.com
- **Entity type:** private_operator
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Hoteles MX
- **Sample properties:**
  - Hotel Estancias VIVE MX el marques Queretaro, Trademark — Santiago de Querétaro — Mexico
  - Siente Tulum & Cenote Club, a Trademark by Wyndham Hotel — Tulum — Mexico
  - Hotel MX Más San Miguel de Allende — San Miguel de Allende — Mexico
  - Hotel Estancias VIVE MX Cuauhtemoc, Trademark by Wyndham — Mexico City — Mexico
  - Hotel MX Congreso, Trademark Collection by Wyndham — Mexico City — Mexico
- **Hints:**
  - CoStar True Owner: Hoteles MX
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

### Pulso Inmobiliario (A, P1)

- **Status:** needs_registry
- **Country:** Mexico
- **Registry:** Mexico — corporate web / IR first [SIGER optional]
- **Bridge:** direct_entity
- **CALA properties:** 8
- **Primary path:** corporate_web_first
- **Corporate site:** —
- **Entity type:** private_operator
- **SIGER fallback (optional):** https://www.siger.gob.mx/
- **Entity search:** Pulso Inmobiliario
- **Sample properties:**
  - Secrets Vallarta Bay Puerto Vallarta — Puerto Vallarta — Mexico
  - Secrets Riviera Cancun Resort & Spa — Puerto Morelos — Mexico
  - Dreams Riviera Cancun Resort & Spa — Puerto Morelos — Mexico
  - Dreams Vallarta Bay Resort & Spa — Puerto Vallarta — Mexico
  - Dreams Natura Resort & Spa — Cancun — Mexico
- **Hints:**
  - CoStar True Owner: Pulso Inmobiliario
  - Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).
- **Registry notes:**
  - WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.
  - Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.
  - Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.
  - Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).
  - Optional — SIGER only if you have CURP and need V1R legal-rep proof.
  - RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.
  - SPV owners: hotel website footer → operating entity → corporate site.

## Enrichment output

Save completed lookups to `data/internal/gtm-registry-enrichments/*.json` and run:

```bash
node scripts/import-gtm-registry-contact-enrichments.mjs --dry-run
node scripts/import-gtm-registry-contact-enrichments.mjs --apply
node scripts/sync-gtm-owner-target-contacts.mjs --apply
```