import {CHOICES, randomId, roundKey, cleanName, createMatch, applyCommit, applyReveal, applyReady, applyRematch, validState, digestChoice, winner} from './motor.mjs';

const $ = id => document.getElementById(id);
const labels = {rock:'Taş',paper:'Kâğıt',scissors:'Makas'};
const choiceButtons = [...document.querySelectorAll('[data-choice]')];
const SESSION_KEY = 'tkm.session.v1';
let mode=null, room='', host=false, state=null, peer=null, conn=null, me='p1';
let guestToken=null, myToken=null, pending=null, revealSent=null, busyChoice=false;
let networkReady=false, opponentConnected=false, statusMessage='', generation=0, retryTimer=null, toastTimer=null;
let joined=false, queue=Promise.resolve(), sessionRestored=false;
const safeRead = (store,key) => {try{return store.getItem(key)}catch{return null}};
const safeWrite = (store,key,value) => {try{store.setItem(key,value)}catch{}};
const savedName=safeRead(localStorage,'tkm.name');
if(savedName) $('name').value=cleanName(savedName);
const cleanRoom=value=>String(value||'').toUpperCase().replace(/[^A-HJ-NP-Z2-9]/g,'').slice(0,8);
const roomOK=value=>/^[A-HJ-NP-Z2-9]{7}$/.test(value);
const active=()=>mode==='online';
const myName=()=>cleanName($('name').value||'Oyuncu');
const clone=value=>JSON.parse(JSON.stringify(value));

function toast(message){$('toast').textContent=message;$('toast').hidden=false;clearTimeout(toastTimer);toastTimer=setTimeout(()=>$('toast').hidden=true,3500)}
function save(){
  if(!active())return;
  safeWrite(sessionStorage,SESSION_KEY,JSON.stringify({room,host,token:myToken,guestToken,state,pending,name:myName()}));
}
function status(message){statusMessage=message;render()}
function closeNetwork(){
  generation++;clearTimeout(retryTimer);retryTimer=null;
  const oldPeer=peer;peer=null;conn=null;joined=false;networkReady=false;opponentConnected=false;
  if(oldPeer)oldPeer.destroy();
}
function resetLobby(){
  closeNetwork();mode=null;state=null;pending=null;revealSent=null;busyChoice=false;
  try{sessionStorage.removeItem(SESSION_KEY)}catch{}
  const url=new URL(location.href);url.search='';url.hash='';history.replaceState(null,'',url);
  render();
}
function send(message){if(conn?.open)conn.send(message)}
function sync(){save();render();if(host&&conn?.open&&joined)send({type:'STATE',state:clone(state)});maybeReveal()}
function validMessage(data){return data&&typeof data==='object'&&!Array.isArray(data)&&typeof data.type==='string'&&data.type.length<20}

function connectHost(){
  closeNetwork();const gen=generation;me='p1';joined=false;
  statusMessage='Oda bağlantısı kuruluyor…';render();
  peer=new Peer('tkm-v1-'+room,{});
  peer.on('open',()=>{if(gen!==generation)return;networkReady=true;status(opponentConnected?'İkiniz de buradasınız · düello canlı':'Oda açık · arkadaşın bekleniyor')});
  peer.on('disconnected',()=>{if(gen!==generation)return;networkReady=false;opponentConnected=false;status('İnternet bağlantısı bekleniyor…')});
  peer.on('reconnected',()=>{if(gen!==generation)return;networkReady=true;opponentConnected=!!(joined&&conn?.open);status(opponentConnected?'İkiniz de buradasınız · düello canlı':'Oda açık · arkadaşın bekleniyor');sync()});
  peer.on('error',()=>{if(gen!==generation)return;networkReady=false;status('Bağlantı kurulamadı. Yeniden deneyebilirsin.')});
  peer.on('connection',incoming=>{
    if(gen!==generation){incoming.close();return}
    let authenticated=false;
    const authenticationTimeout=setTimeout(()=>{if(!authenticated)incoming.close()},12000);
    incoming.on('data',data=>{
      queue=queue.then(async()=>{
        if(gen!==generation||!validMessage(data))return;
        if(data.type==='JOIN'){
          if(typeof data.token!=='string'||!/^[0-9a-f]{64}$/.test(data.token)||typeof data.name!=='string'||data.name.length>24)return;
          if(guestToken&&guestToken!==data.token){incoming.send({type:'REJECT',reason:'Bu oda başka bir oyuncuyla eşleşti. Yeni bir oda daveti iste.'});setTimeout(()=>incoming.close(),500);return}
          authenticated=true;clearTimeout(authenticationTimeout);
          if(conn&&conn!==incoming)conn.close();
          conn=incoming;guestToken=data.token;joined=true;opponentConnected=true;networkReady=true;revealSent=null;
          const name=cleanName(data.name);if(state.names.p2!==name){state.names.p2=name;state.revision++}
          statusMessage='İkiniz de buradasınız · düello canlı';sync();return;
        }
        if(!authenticated||conn!==incoming||!joined)return;
        let changed=false;
        if(data.type==='COMMIT')changed=applyCommit(state,'p2',data);
        if(data.type==='REVEAL')changed=await applyReveal(state,'p2',data);
        if(data.type==='READY')changed=applyReady(state,'p2',data);
        if(data.type==='REMATCH'){
          changed=applyRematch(state,'p2',data);
          if(changed&&state.rematch.p1&&state.rematch.p2){state=createMatch(state.names);pending=null;revealSent=null}
        }
        if(data.type==='RESYNC'){sync();return}
        if(changed)sync();
      }).catch(()=>{status('Hamle işlenemedi. Yeniden bağlanmayı deneyebilirsin.')});
    });
    incoming.on('close',()=>{
      clearTimeout(authenticationTimeout);
      if(gen!==generation||conn!==incoming)return;
      conn=null;joined=false;opponentConnected=false;revealSent=null;
      status('Arkadaşının geri dönmesi bekleniyor…');save();
    });
  });
}
async function verifiedSnapshot(next){
  if(!validState(next))return false;
  if(state&&next.id===state.id){
    if(next.revision<state.revision||next.round<state.round)return false;
    if(next.round===state.round){for(const p of ['p1','p2']){if(state.commits[p]&&state.commits[p]!==next.commits[p])return false}}
  }else if(state&&state.phase!=='over')return false;
  if(next.result){
    for(const p of ['p1','p2']){const reveal=next.reveals[p];if(await digestChoice(next.id,next.round,p,reveal.choice,reveal.nonce)!==next.commits[p])return false}
    if(next.result.p1!==next.reveals.p1.choice||next.result.p2!==next.reveals.p2.choice||next.result.winner!==winner(next.result.p1,next.result.p2))return false;
  }
  if(pending&&pending.key===roundKey(next)&&next.commits[me]&&next.commits[me]!==pending.hash)return false;
  return true;
}
function connectGuest(){
  closeNetwork();const gen=generation;me='p2';statusMessage='Oda sahibine bağlanılıyor…';render();
  peer=new Peer({});
  peer.on('open',()=>{
    if(gen!==generation)return;networkReady=true;
    conn=peer.connect('tkm-v1-'+room);
    const link=conn;
    conn.on('open',()=>{if(gen!==generation)return;networkReady=true;revealSent=null;send({type:'JOIN',name:myName(),token:myToken});status('Odaya katılınıyor…')});
    conn.on('data',data=>{
      queue=queue.then(async()=>{
        if(gen!==generation||link!==conn||!validMessage(data))return;
        if(data.type==='REJECT'){joined=false;opponentConnected=false;statusMessage=data.reason||'Bu oda dolu.';toast(statusMessage);closeNetwork();status(statusMessage);return}
        if(data.type!=='STATE')return;
        if(!await verifiedSnapshot(data.state)){toast('Oyun durumu doğrulanamadı. Yeniden bağlanmayı dene.');return}
        const next=data.state;
        if(state&&roundKey(state)!==roundKey(next)){pending=null;revealSent=null}
        state=next;joined=true;opponentConnected=true;networkReady=true;statusMessage='İkiniz de buradasınız · düello canlı';
        save();render();
        if(pending&&pending.key===roundKey(state)&&state.phase==='choose'&&!state.commits[me])send({type:'COMMIT',id:state.id,round:state.round,hash:pending.hash});
        maybeReveal();
      }).catch(()=>status('Bağlantı yenileniyor…'));
    });
    conn.on('stale',()=>{if(gen!==generation)return;opponentConnected=false;status('Oda sahibinin geri dönmesi bekleniyor…')});
    conn.on('fresh',()=>{if(gen!==generation)return;networkReady=true;opponentConnected=joined;status('Bağlantı yeniden kuruldu');send({type:'RESYNC'})});
    conn.on('close',()=>{
      if(gen!==generation)return;opponentConnected=false;joined=false;revealSent=null;
      status('Oda bağlantısı kesildi · yeniden deneniyor…');
      if(active())retryTimer=setTimeout(()=>{if(gen===generation)connectGuest()},1800);
    });
  });
  peer.on('error',()=>{if(gen!==generation)return;networkReady=false;status('Oda sahibine ulaşılamadı. Oyun sekmesini açık tutmasını iste.')});
  peer.on('disconnected',()=>{if(gen!==generation)return;networkReady=false;opponentConnected=false;status('İnternet bağlantısı bekleniyor…')});
  peer.on('reconnected',()=>{
    if(gen!==generation)return;
    networkReady=true;opponentConnected=!!(joined&&conn?.open);
    status(opponentConnected?'Bağlantı yeniden kuruldu':'Oda sahibine yeniden bağlanılıyor…');
    if(conn?.open)send(joined?{type:'RESYNC'}:{type:'JOIN',name:myName(),token:myToken});
  });
}
function openRoom(asHost,code,restored=null){
  mode='online';host=asHost;room=code;me=host?'p1':'p2';
  myToken=restored?.token||randomId(32);guestToken=restored?.guestToken||null;
  state=restored?.state&&validState(restored.state)?restored.state:(host?createMatch({p1:myName(),p2:'Arkadaşın'}):null);
  pending=restored?.pending||null;revealSent=null;busyChoice=false;
  safeWrite(localStorage,'tkm.name',myName());
  const url=new URL(location.href);url.search='';url.hash='';url.searchParams.set('oda',room);history.replaceState(null,'',url);
  save();if(host)connectHost();else connectGuest();
}
function makeRoom(){const alphabet='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';const bytes=crypto.getRandomValues(new Uint8Array(7));return Array.from(bytes,n=>alphabet[n%alphabet.length]).join('')}
function startSolo(){
  closeNetwork();mode='solo';host=true;me='p1';pending=null;busyChoice=false;revealSent=null;
  state=createMatch({p1:myName(),p2:'Bot'});safeWrite(localStorage,'tkm.name',myName());statusMessage='Isınma turu · üç canlık maç';render();
}
async function maybeReveal(){
  if(!active()||!state||state.phase!=='reveal'||!pending||pending.key!==roundKey(state)||revealSent===pending.key||!opponentConnected)return;
  if(pending.hash!==state.commits[me])return;
  revealSent=pending.key;
  const reveal={type:'REVEAL',id:state.id,round:state.round,choice:pending.choice,nonce:pending.nonce};
  if(host){const current=state;if(await applyReveal(current,'p1',reveal)&&state===current)sync()}
  else send(reveal);
}
async function choose(choice){
  if(!CHOICES.includes(choice)||busyChoice||!state||state.phase!=='choose'||state.commits[me]||(pending&&pending.key===roundKey(state))||(active()&&!opponentConnected))return;
  busyChoice=true;const match=state;const key=roundKey(state);const nonce=randomId(32);
  pending={choice,nonce,key,hash:await digestChoice(state.id,state.round,me,choice,nonce)};save();render();
  if(state!==match||mode===null){busyChoice=false;return}
  const message={type:'COMMIT',id:state.id,round:state.round,hash:pending.hash};
  if(active()){
    if(host){if(applyCommit(state,'p1',message))sync()}else send(message);
    busyChoice=false;render();return;
  }
  const botChoice=CHOICES[crypto.getRandomValues(new Uint8Array(1))[0]%3];const botNonce=randomId(32);
  applyCommit(state,'p1',message);
  applyCommit(state,'p2',{id:state.id,round:state.round,hash:await digestChoice(state.id,state.round,'p2',botChoice,botNonce)});
  render();
  await new Promise(resolve=>setTimeout(resolve,550));
  if(mode!=='solo'||state!==match){busyChoice=false;return}
  await applyReveal(state,'p1',{id:state.id,round:state.round,choice,nonce});
  await applyReveal(state,'p2',{id:state.id,round:state.round,choice:botChoice,nonce:botNonce});
  busyChoice=false;render();
}
function proceed(){
  if(!state||!['result','over'].includes(state.phase))return;
  const type=state.phase==='over'?'REMATCH':'READY';const message={type,id:state.id,round:state.round};
  if(mode==='solo'){
    if(type==='REMATCH'){state=createMatch(state.names);pending=null;revealSent=null;render();return}
    applyReady(state,'p1',message);applyReady(state,'p2',message);pending=null;revealSent=null;render();return;
  }
  if(!opponentConnected)return;
  if(host){
    const changed=type==='REMATCH'?applyRematch(state,'p1',message):applyReady(state,'p1',message);
    if(changed&&state.rematch.p1&&state.rematch.p2){state=createMatch(state.names);pending=null;revealSent=null}
    if(changed)sync();
  }else send(message);
}
function inviteURL(){const url=new URL(location.href);url.search='';url.hash='';url.searchParams.set('oda',room);return url.href}
function whatsapp(){if(!active()||!room)return;const text=`${myName()} seni Taş Kâğıt Makas düellosuna çağırıyor! ✊ Üç canlık maçımıza katıl:\n${inviteURL()}`;window.open('https://wa.me/?text='+encodeURIComponent(text),'_blank','noopener,noreferrer')}
async function copyInvite(){try{await navigator.clipboard.writeText(inviteURL());toast('Davet bağlantısı kopyalandı')}catch{if(navigator.share){try{await navigator.share({title:'Taş Kâğıt Makas',url:inviteURL()})}catch{}}else toast('Davet için adres çubuğundaki bağlantıyı kopyalayabilirsin.')}}
function setHand(id,choice){const svg=$(id);svg.querySelector('use').setAttribute('href','eller.svg#'+choice);svg.setAttribute('aria-label',labels[choice]||'Gizli hamle')}
function drawHearts(id,count){const el=$(id);el.replaceChildren();for(let i=0;i<3;i++){const heart=document.createElement('span');heart.textContent='♥';if(i>=count)heart.className='lost-heart';el.append(heart);if(i<2)el.append(' ')}el.setAttribute('aria-label',`${count} can`)}
function render(){
  const playing=mode!==null;$('lobby').hidden=playing;$('arena').hidden=!playing;if(!playing)return;
  const online=active(),other=me==='p1'?'p2':'p1';
  $('room-badge').textContent=online?'ODA · '+room:'ISINMA TURU';
  $('invite-top').hidden=!online;$('host-note').hidden=!online;
  $('connection-text').textContent=statusMessage;
  $('connection').classList.toggle('offline',online&&(!networkReady||!opponentConnected));
  $('your-name').textContent=state?.names?.[me]||myName();$('their-name').textContent=state?.names?.[other]||'Arkadaşın';
  drawHearts('your-hearts',state?.lives?.[me]??3);drawHearts('their-hearts',state?.lives?.[other]??3);
  const waiting=online&&host&&!guestToken;
  $('waiting-room').hidden=!waiting;
  $('round-copy').hidden=waiting;
  const result=state&&['result','over'].includes(state.phase), myLocked=!!state?.commits?.[me],theirLocked=!!state?.commits?.[other];
  const chosen=pending&&state&&pending.key===roundKey(state)?pending.choice:null;
  setHand('your-hand',result?state.result[me]:(chosen||'rock'));
  setHand('their-hand',result?state.result[other]:'rock');
  const stage=document.querySelector('.hand-stage');stage.classList.toggle('revealing',state?.phase==='reveal');stage.classList.toggle('result',!!result);
  stage.children[0].classList.toggle('locked',myLocked);stage.children[1].classList.toggle('locked',theirLocked);
  $('your-pick-state').textContent=result?labels[state.result[me]]:myLocked?'✓ Hamlen kilitli':'Hamleni bekliyor';
  $('their-pick-state').textContent=result?labels[state.result[other]]:theirLocked?'✓ Hamlesi kilitli':online&&!opponentConnected?'Bağlantı bekleniyor':'Hamlesi gizli';
  $('your-pick-state').classList.toggle('locked',myLocked);$('their-pick-state').classList.toggle('locked',theirLocked);
  $('choices').hidden=waiting||!!result;
  for(const button of choiceButtons){button.disabled=busyChoice||!!chosen||!state||state.phase!=='choose'||myLocked||(online&&!opponentConnected);button.classList.toggle('selected',chosen===button.dataset.choice)}
  $('retry').hidden=!online||(networkReady&&opponentConnected)||waiting;
  if(online&&host&&waiting&&!networkReady)$('retry').hidden=false;
  $('next').hidden=!result;
  if(!state){$('round-number').textContent='CANLI ODA';$('round-title').textContent='Düelloya katılıyorsun.';$('round-subtitle').textContent='Odayı kuran kişi oyun sekmesini açık tutmalı.';return}
  $('round-number').textContent=`${state.round}. TUR`;
  if(result){
    const won=state.result.winner;
    if(state.phase==='over'){
      const youWon=state.lives[me]>0;
      $('round-title').textContent=youWon?'Düello senin!':'Bu düello rakibinin.';
      $('round-subtitle').textContent=youWon?'Son sözü sen söyledin. Bir rövanş daha?':'Güzel maçtı. Rövanşta işler değişebilir.';
      const voted=state.rematch[me];$('next').textContent=voted?'Rövanş için rakibin bekleniyor…':'Rövanş oyna ↗';$('next').disabled=voted||(online&&!opponentConnected);
    }else{
      $('round-title').textContent=won===null?'Aynı fikirdeydiniz.':won===me?'Bu el senin!':'Bu el rakibinin.';
      $('round-subtitle').textContent=won===null?'Berabere. Kimse can kaybetmedi.':`${labels[state.result[won]]}, ${labels[state.result[won==='p1'?'p2':'p1']].toLocaleLowerCase('tr')} hamlesini yendi.`;
      $('next').textContent=state.ready[me]?'Rakibinin hazır olması bekleniyor…':'Sonraki tur →';$('next').disabled=state.ready[me]||(online&&!opponentConnected);
    }
  }else if(online&&!opponentConnected&&!waiting){$('round-title').textContent='Düello seni bekliyor.';$('round-subtitle').textContent='Bağlantı geri gelince kaldığınız yerden devam.'}
  else if(myLocked){$('round-title').textContent=theirLocked?'Eller açılıyor…':'Hamlen aramızda.';$('round-subtitle').textContent=theirLocked?'Taş, kâğıt, makas!':'Rakibin de seçince aynı anda açılacak.'}
  else{$('round-title').textContent='İçgüdüne güven.';$('round-subtitle').textContent='Bir hamle seç. İkiniz de seçince eller açılır.'}
}

$('theme').addEventListener('click',()=>{const next=document.documentElement.dataset.theme==='dark'?'light':'dark';document.documentElement.dataset.theme=next;safeWrite(localStorage,'tkm.theme',next);$('theme').setAttribute('aria-label',next==='dark'?'Açık temaya geç':'Koyu temaya geç')});
$('create').addEventListener('click',()=>{if(!crypto.subtle){toast('Çevrimiçi oyun için HTTPS bağlantısını kullanmalısın.');return}openRoom(true,makeRoom())});
$('join').addEventListener('click',()=>{const code=cleanRoom($('room').value);if(!roomOK(code)){toast('Yedi karakterli oda kodunu yaz.');$('room').focus();return}if(!crypto.subtle){toast('Çevrimiçi oyun için HTTPS bağlantısını kullanmalısın.');return}openRoom(false,code)});
$('room').addEventListener('input',()=>{$('room').value=cleanRoom($('room').value)});
$('room').addEventListener('keydown',event=>{if(event.key==='Enter')$('join').click()});
$('solo').addEventListener('click',startSolo);$('leave').addEventListener('click',resetLobby);
$('whatsapp').addEventListener('click',whatsapp);$('invite-top').addEventListener('click',whatsapp);$('copy').addEventListener('click',copyInvite);
$('next').addEventListener('click',proceed);$('retry').addEventListener('click',()=>{if(host)connectHost();else connectGuest()});
for(const button of choiceButtons)button.addEventListener('click',()=>choose(button.dataset.choice).catch(()=>{busyChoice=false;toast('Hamle gönderilemedi. Yeniden deneyebilirsin.');render()}));
window.addEventListener('pagehide',save);
window.addEventListener('online',()=>{if(!active())return;if(host){peer?.reconnect()}else if(!conn?.open)connectGuest()});
document.addEventListener('visibilitychange',()=>{if(!document.hidden&&active()){peer?.reconnect();if(!host&&conn?.open)send({type:'RESYNC'})}});
if('serviceWorker' in navigator&&location.protocol!=='file:')navigator.serviceWorker.register('./sw.js').catch(()=>{});
const invitedRoom=cleanRoom(new URL(location.href).searchParams.get('oda'));
let restored;try{restored=JSON.parse(safeRead(sessionStorage,SESSION_KEY)||'null')}catch{}
if(restored&&roomOK(restored.room)&&typeof restored.host==='boolean'&&/^[0-9a-f]{64}$/.test(restored.token||'')&&(!invitedRoom||invitedRoom===restored.room)){
  $('name').value=cleanName(restored.name);sessionRestored=true;openRoom(restored.host,restored.room,restored);
}else if(roomOK(invitedRoom)){
  $('room').value=invitedRoom;
  if(savedName&&crypto.subtle)openRoom(false,invitedRoom);else{toast('Adını yazıp Katıl’a bas; oda kodun hazır.');$('name').focus()}
}else render();
