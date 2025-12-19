
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

### Integracijski testi
Za preizkus verižne replikacije je treba zagnati tri strežnike in jih konfigurirati:

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

1. Zaženi integracijske teste (mapa `/server`):
   ```powershell
   # Zaženi vse teste
   go test -v
   
   # Zaženi izbrane teste
   go test -v -run "TestIDConsistency|TestMessagePropagation"
   
   # Zaženi benchmarke
   go test -bench=.
   ```
