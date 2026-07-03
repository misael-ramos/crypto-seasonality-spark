# Figurinha Lendária #33 — Yasmin Belo

Experiência interativa romântica para ser acessada via QR Code no álbum de figurinhas personalizado.

## Estrutura de arquivos

```
yasmin-album/
├── index.html        ← página principal (tudo aqui)
├── images/
│   ├── foto1.jpg     ← adicione suas fotos aqui
│   ├── foto2.jpg
│   ├── foto3.jpg
│   ├── foto4.jpg
│   └── foto5.jpg
└── audio/
    └── lembrei-de-nos.mp3  ← adicione a música aqui
```

## Como publicar no GitHub Pages

1. Crie um repositório no GitHub (ex: `yasmin-album`)
2. Faça upload de todos os arquivos
3. Vá em Settings → Pages → Branch: main → Save
4. Acesse: `https://seu-usuario.github.io/yasmin-album`
5. Gere um QR Code com esse link e cole na última página do álbum

## Como adicionar fotos

Edite o array `photos` no JavaScript do `index.html`:
```js
const photos = [
  "images/foto1.jpg",
  "images/foto2.jpg",
  // adicione quantas quiser
];
```

## Como adicionar a música

Coloque o arquivo MP3 em `audio/lembrei-de-nos.mp3`
