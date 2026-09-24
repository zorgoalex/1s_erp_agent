# Contract mock

Локальный mock одновременно реализует ERP Agent API, HTTP-сервис 1С и минимальный OData endpoint. Он предназначен только для разработки и E2E-проверок на loopback-интерфейсе.

```powershell
dotnet run --project tools/ErpOnecAgent.MockServer
```

- `POST /mock/commands` — поставить command envelope в очередь ERP.
- `GET /mock/results/{commandId}` — прочитать доставленный результат.
- `POST /mock/odata/{entity}` — задать JSON-массив строк сущности.
