# Luffy Lite

نسخه‌ی سبک پنل: یک کاربر ثابت و صفحه‌ای که فقط لینک VLESS (WS + TLS) را نشان می‌دهد. ساخته‌شده با Reflex.

## اجرا

```bash
pip install -r requirements.txt
reflex run
```

## دیپلوی

```bash
reflex login
reflex deploy --env VLESS_UUID=YOUR-UUID
```

ساخت UUID:

```bash
python -c "import uuid;print(uuid.uuid4())"
```

## متغیرهای محیطی

| نام | توضیح | پیش‌فرض |
|---|---|---|
| `VLESS_UUID` | UUID کاربر | ساخت خودکار و ذخیره در `uuid.txt` |
| `VLESS_NAME` | اسم لینک و انتهای path | `Luffy` |
| `VLESS_HOST` | دامنه‌ی لینک | دامنه‌ی خود صفحه |
