
# Testiranje verižne replikacije

## Pokritost testov

Testi zajemajo:

### Osnovna funkcionalnost
- **TestServerCreation**: Inicializacija strežnika in privzeto stanje
- **TestChainConfiguration**: Pravilna nastavitev verige (head, middle, tail)
- **TestWriteOnlyAtHead**: Pisalne operacije so dovoljene samo na head vozlišču
- **TestReadOnlyAtTail**: Bralne operacije so dovoljene samo na tail vozlišču

### Replikacija verige
- **TestIDConsistency**: Preveri skladnost generiranja ID-jev po celotni verigi
- **TestTopicPropagation**: Preveri razširjanje ustvarjanja tem skozi verigo
- **TestMessagePropagation**: Preveri razširjanje objav sporočil
- **TestMessageUpdatePropagation**: Preveri razširjanje posodobitev sporočil
- **TestMessageDeletePropagation**: Preveri razširjanje brisanja sporočil
- **TestLikePropagation**: Preveri razširjanje števila všečkov

### Zaporedne številke in potrditve (Sequence Numbers & Acknowledgments)
- **TestSequenceNumbers**: Preveri dodeljevanje in monotono naraščanje zaporednih številk za vse operacije
- **TestCommittedFlag**: Preveri, da se committed flag nastavi na true po potrditvi od tail vozlišča
- **TestAcknowledgmentTimeout**: Testira timeout vedenje (5 sekund), ko tail ne odgovori
- **TestAcknowledgmentPropagation**: Preveri, da se potrditve širijo nazaj skozi verigo od tail do head

### Validacija zaporednih številk
- **TestSequenceValidation**: Testira zavrnitev operacij z zaporednimi številkami, ki preskočijo vrednosti (preverjanje vrstnega reda)

### Zaznavanje head/tail z eksplicitnimi naslovi
- **TestHeadTailDetectionWithAddresses**: Preveri zaznavanje head/tail z uporabo eksplicitnih naslovov
  - Simulira odpoved srednjih vozlišč (nil successor)
  - Zagotavlja pravilno delovanje zaznave tudi po odpovedi vozlišča

### Posebni primeri
- **TestSingleNodeChain**: Testira enojno vozlišče, ki deluje kot head in tail
  - Preveri takojšnjo potrditev brez čakanja
  - Zagotavlja delovanje pisalnih in bralnih operacij

### Robni primeri in validacija
- **TestUpdateAuthorization**: Samo avtor sporočila lahko posodobi svoje sporočilo
- **TestNonExistentUser**: Pravilno obravnava neobstoječih uporabnikov
- **TestNonExistentTopic**: Pravilno obravnava neobstoječih tem
- **TestGetMessagesFiltering**: Pridobivanje sporočil z uporabo `from_id` in `limit`
- **TestUsernameUniqueness**: Preprečuje ponavljanje uporabniških imen
- **TestEmptyMessageValidation**: Zavrača prazna besedila sporočil

### Porazdelitev naročnin (Load balancing)
- **TestSubscriptionToken**: Generiranje tokenov za naročnine
- **TestLoadBalancing**: Porazdelitev naročnin med vozlišča
- **TestSubscriptionWithBacklog**: Dostava backlogov novim naročnikom

### Integracija
- **TestFullWorkflow**: Celoten potek (ustvari → objavi → všečkaj → posodobi → izbriši)
- **BenchmarkMessagePropagation**: Meritve izvedbe

## Zagon testov

### Control Panel
Control panel zdaj avtomatsko ustvari in konfigurira verigo strežnikov:

1. **Zagon control panela** (privzeto 3 strežniki, začetek pri portu 50051):
   ```powershell
   cd control-panel
   go run control-panel.go
   ```

2. **Zagon s parametri**:
   ```powershell
   # 5 strežnikov, začetek pri portu 60000
   go run control-panel.go -n 5 -p 60000
   ```

3. **Zagon klienta** (v drugem terminalu):
   ```powershell
   cd client
   go run client.go
   ```

### Integracijski testi (ročno)
Za preizkus verižne replikacije lahko ročno zaženete strežnike:

1. **Terminal 1** - Zaženi head vozlišče:
   ```powershell
   go run server.go -n node-1 -a 50051
   ```

2. **Terminal 2** - Zaženi middle vozlišče:
   ```powershell
   go run server.go -n node-2 -a 50052
   ```

3. **Terminal 3** - Zaženi tail vozlišče:
   ```powershell
   go run server.go -n node-3 -a 50053
   ```

4. **Terminal 4** - Konfiguriraj verigo:
   ```powershell
   go run control-panel.go
   ```

5. **Terminal 5** - Zaženi klienta:
   ```powershell
   go run client.go
   ```

### Avtomatsko testiranje

1. **Zaženi strežniške teste** (mapa `/server`):
   ```powershell
   # Zaženi vse teste
   go test -v
   
   # Zaženi izbrane teste
   go test -v -run "TestSequence|TestAck|TestHeadTail"
   
   # Zaženi benchmarke
   go test -bench=.
   ```

2. **Zaženi control panel teste** (mapa `/control-panel`):
   ```powershell
   # Zaženi vse teste
   go test -v
   
   # Zaženi specifične teste
   go test -v -run "TestStartServers|TestConfigureChain"
   ```

## Nove funkcionalnosti v testih

### Two-Phase Commit s potrditvami
Testi preverjajo:
- Dodeljevanje zaporednih številk vsem operacijam
- Čakanje na potrditev od tail vozlišča (5 sekund timeout)
- Nastavljanje `committed` flag na true po potrditvi
- Širjenje potrditev nazaj od tail do head
- Obravnavanje timeoutov (operacija se dokonča z `committed=false`)

### Validacija zaporednih številk
Testi preverjajo:
- Zavrnitev operacij, ki preskočijo zaporedne številke
- Preverjanje, da operacije prispejo v vrstnem redu
- Zaščito pred neusklajenostjo zaradi manjkajočih operacij

### Eksplicitno zaznavanje head/tail
Testi preverjajo:
- Zaznavanje head in tail z uporabo eksplicitnih naslovov
- Pravilno delovanje tudi pri odpovedi srednjih vozlišč
- Preprečevanje napak, kjer več vozlišč misli, da so tail

### Control Panel
Testi preverjajo:
- Ustvarjanje in zagon strežniških procesov
- Dodeljevanje portov
- Konfiguracijo verige na vseh vozliščih
- Obravnavanje napak pri zagonu
- Podporo za različne velikosti verig (1-5+ vozlišč)
