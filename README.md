# Taş Kâğıt Makas · El Düellosu

Arkadaşınla tarayıcıdan canlı düello: üç can, gizli seçimler, beraberlik ve rövanş. Botla ısınma modu da vardır. Tasarım ve SVG eller bu proje için hazırlanmıştır.

## Birlikte oyna

1. Oyuncu adını yaz ve **Arkadaşına meydan oku** düğmesine bas.
2. **WhatsApp’tan davet et** ile oda bağlantısını arkadaşına gönder.
3. Arkadaşın bağlantıyı açar, adını yazar ve **Katıl** der.
4. Her oyuncu taş, kâğıt veya makas seçer. İkiniz de seçince eller açılır; kaybeden bir can kaybeder. Aynı seçimde can kaybı olmaz.
5. Bir oyuncunun üç canı da bitince maç tamamlanır. Rövanş için ikiniz de onay verebilirsiniz.

Odayı kuran kişinin oyun sekmesini açık tutması gerekir. Telefonun tarayıcısı arka planda uyutulursa oyuna dönünce bağlantı toparlanır; gerekirse **Yeniden bağlan** düğmesini kullanın. Aynı sekmede sayfayı yenilemek odayı ve gizli hamleyi korur.

## GitHub Pages

Derleme veya sunucu kurulumu gerekmez. Dosyaları deponun köküne yükleyin; **Settings → Pages → Deploy from a branch → main → /(root)** seçin. Tüm dosya ve davet yolları proje alt yoluyla uyumludur.

Çevrimiçi oyun HTTPS ve Web Crypto kullanır. Sunucu rolünü oda sahibinin tarayıcısı üstlenir; şifreli mesajlar dört halka açık MQTT aktarma hizmetinden iletilir. Bu hizmetlerin kullanılabilirliği dış sağlayıcılara bağlıdır. Hesap veya API anahtarı gerekmez.

## Seçimlerin gizliliği

Oyuncular önce SHA-256 ile oyun, tur, oyuncu, seçim ve rastgele gizli değerden üretilmiş taahhüt gönderir. Her iki taahhüt sabitlendikten sonra seçimler açılır ve eşleşme doğrulanır. Aynı turda hamle değişikliği, eski tur hamlesi ve tekrar teslim edilmiş mesajlar işlenmez.

`motor.mjs` oyun kurallarını, `app.js` arayüz ve oda yönetimini, `oyun-ag.js` şifreli aktarımı içerir. Ağ katmanı SOS oyunundaki mevcut aktarım uygulamasından alınmıştır.
