package notifier

type Stats struct {
	Sent    int64 // успешно отправлено
	Failed  int64 // завершилось ошибкой
	Retries int64 // количество повторных попыток
}
