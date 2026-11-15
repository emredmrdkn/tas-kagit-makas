export const CHOICES = ['rock', 'paper', 'scissors'];
const hex = value => Array.from(value, n => n.toString(16).padStart(2, '0')).join('');
export const randomId = (size = 16) => hex(crypto.getRandomValues(new Uint8Array(size)));
export const roundKey = state => `${state.id}:${state.round}`;
export const isChoice = value => CHOICES.includes(value);
export const cleanName = value => String(value || '').trim().slice(0, 24) || 'Oyuncu';
export function winner(a, b) {
  if (!isChoice(a) || !isChoice(b)) throw new Error('Geçersiz hamle');
  if (a === b) return null;
  return ({rock:'scissors', paper:'rock', scissors:'paper'})[a] === b ? 'p1' : 'p2';
}
export async function digestChoice(id, round, player, choice, nonce) {
  const bytes = new TextEncoder().encode(JSON.stringify([id, round, player, choice, nonce]));
  return hex(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)));
}
export function createMatch(names = {p1:'Sen', p2:'Arkadaşın'}) {
  return {id:randomId(), revision:0, round:1, phase:'choose', names:{p1:cleanName(names.p1),p2:cleanName(names.p2)},
    lives:{p1:3,p2:3}, commits:{p1:null,p2:null}, reveals:{p1:null,p2:null},
    result:null, ready:{p1:false,p2:false}, rematch:{p1:false,p2:false}};
}
const playerOK = player => player === 'p1' || player === 'p2';
export function applyCommit(state, player, message) {
  if (!playerOK(player) || !message || message.id !== state.id || message.round !== state.round ||
    state.phase !== 'choose' || state.commits[player] || !/^[0-9a-f]{64}$/.test(message.hash || '')) return false;
  state.commits[player] = message.hash;
  if (state.commits.p1 && state.commits.p2) state.phase = 'reveal';
  state.revision++;
  return true;
}
export async function applyReveal(state, player, message) {
  if (!playerOK(player) || !message || message.id !== state.id || message.round !== state.round ||
    state.phase !== 'reveal' || state.reveals[player] || !isChoice(message.choice) ||
    !/^[0-9a-f]{64}$/.test(message.nonce || '')) return false;
  const currentId = state.id, currentRound = state.round;
  const hash = await digestChoice(state.id, state.round, player, message.choice, message.nonce);
  if (state.id !== currentId || state.round !== currentRound || state.phase !== 'reveal' || state.reveals[player] || hash !== state.commits[player]) return false;
  state.reveals[player] = {choice:message.choice,nonce:message.nonce};
  state.revision++;
  if (state.reveals.p1 && state.reveals.p2) {
    const won = winner(state.reveals.p1.choice, state.reveals.p2.choice);
    if (won) state.lives[won === 'p1' ? 'p2' : 'p1']--;
    state.result = {winner:won,p1:state.reveals.p1.choice,p2:state.reveals.p2.choice};
    state.phase = state.lives.p1 === 0 || state.lives.p2 === 0 ? 'over' : 'result';
  }
  return true;
}
export function applyReady(state, player, message) {
  if (!playerOK(player) || !message || message.id !== state.id || message.round !== state.round ||
    state.phase !== 'result' || state.ready[player]) return false;
  state.ready[player] = true;
  state.revision++;
  if (state.ready.p1 && state.ready.p2) {
    state.round++;
    state.phase='choose'; state.commits={p1:null,p2:null}; state.reveals={p1:null,p2:null};
    state.result=null; state.ready={p1:false,p2:false};
  }
  return true;
}
export function applyRematch(state, player, message) {
  if (!playerOK(player) || !message || message.id !== state.id || state.phase !== 'over' || state.rematch[player]) return false;
  state.rematch[player]=true; state.revision++;
  return true;
}
export function validState(state) {
  if (!state || typeof state !== 'object' || !/^[0-9a-f]{32}$/.test(state.id || '') ||
    !Number.isSafeInteger(state.revision) || state.revision < 0 || !Number.isSafeInteger(state.round) || state.round < 1 ||
    !['choose','reveal','result','over'].includes(state.phase)) return false;
  for (const p of ['p1','p2']) {
    if (typeof state.names?.[p] !== 'string' || state.names[p].length > 24 ||
      !Number.isInteger(state.lives?.[p]) || state.lives[p] < 0 || state.lives[p] > 3 ||
      !(state.commits?.[p] === null || /^[0-9a-f]{64}$/.test(state.commits?.[p] || '')) ||
      typeof state.ready?.[p] !== 'boolean' || typeof state.rematch?.[p] !== 'boolean') return false;
    if (state.reveals?.[p] !== null && (!isChoice(state.reveals?.[p]?.choice) || !/^[0-9a-f]{64}$/.test(state.reveals?.[p]?.nonce || ''))) return false;
  }
  if (state.result !== null && (!isChoice(state.result?.p1) || !isChoice(state.result?.p2) || ![null,'p1','p2'].includes(state.result?.winner))) return false;
  if (['result','over'].includes(state.phase) && (!state.result || !state.reveals.p1 || !state.reveals.p2)) return false;
  if (state.phase === 'over' && state.lives.p1 !== 0 && state.lives.p2 !== 0) return false;
  return true;
}
