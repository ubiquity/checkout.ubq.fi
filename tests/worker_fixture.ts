/** Isolated protocol/RPC/relay fixture, executed inside workerd, with no network. */
export const fixtureScript = `
const entries = new Map();
let delivery = null;
const deliveries = [];
const methods = {};
let transfer = null;
let version = 0;
let writes = 0;
let rpcCalls = 0;
let kvCalls = 0;
const encode = key => "u1|" + key.map(p => typeof p === "string" ? "s"+p.length+":"+p : typeof p === "number" ? "n:"+p : "b:"+(p?1:0)).join("|");
const get = key => { const entry=entries.get(JSON.stringify(key)); return entry && (!entry.expires || entry.expires>Date.now()) ? entry : null; };
const set = (key,value,expireInMs) => { const entry={key,value,versionstamp:String(version),expires:expireInMs?Date.now()+expireInMs:null}; entries.set(JSON.stringify(key),entry); };
export default {
  async fetch(request) {
    const url=new URL(request.url);
    if(url.hostname==="fixture.invalid"&&url.pathname==="/rpc-fixture") { transfer=await request.json(); return Response.json({status:"configured"}); }
    if(url.pathname==="/relay-log") return Response.json(delivery);
    if(url.pathname==="/relay-ledger") return Response.json(deliveries);
    if(url.hostname==="cloudflare-dns.com") return Response.json({Status:0,Answer:url.searchParams.get("type")==="A"?[{type:1,data:"93.184.216.34"}]:[]});
    if(url.hostname==="merchant.example") { delivery={url:request.url,...await request.json()}; deliveries.push(delivery); return new Response(null,{status:204}); }
    if(url.pathname==="/stats") return Response.json({rpcCalls,kvCalls,writes,methods,entries:entries.size});
    if(url.hostname==="rpc.ubq.fi") {
      rpcCalls++;
      const calls=await request.json();
      for(const call of calls) methods[call.method]=(methods[call.method]??0)+1;
      const blockHash=transfer?.blockHash??("0x"+"ab".repeat(32));
      const block=number=>({number:"0x"+number.toString(16),hash:transfer?.canonicalHash??blockHash,timestamp:"0x"+(transfer?.includedAt??Math.floor(Date.now()/1000)).toString(16)});
      const blockNumber=transfer?BigInt(transfer.blockNumber):100n;
      const hexBlock="0x"+blockNumber.toString(16);
      const receipt=transfer?{transactionHash:transfer.transactionHash,blockNumber:hexBlock,blockHash,status:"0x1",logs:[{address:transfer.tokenAddress,transactionHash:transfer.transactionHash,blockNumber:hexBlock,blockHash,logIndex:"0x"+Number(transfer.logIndex??7).toString(16),removed:false,topics:["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x"+"0".repeat(24)+"33".repeat(20),"0x"+"0".repeat(24)+transfer.treasuryAddress.slice(2)],data:"0x"+BigInt(transfer.amountUnits).toString(16).padStart(64,"0")}]}:null;
      return Response.json(calls.map(call=>({id:call.id,jsonrpc:"2.0",result:
        call.method==="eth_getLogs" ? (receipt&&BigInt(call.params[0].fromBlock)<=blockNumber&&BigInt(call.params[0].toBlock)>=blockNumber?receipt.logs:[]) :
        call.method==="eth_getTransactionReceipt" ? receipt :
        transfer?.unavailableHeadTags&&(call.id==="safe"||call.id==="finalized") ? null :
        block(transfer?(call.id==="event_block"?blockNumber:call.id==="latest"?blockNumber+3n:call.id==="safe"&&transfer.safeBlock!==undefined?BigInt(transfer.safeBlock):blockNumber-1n):100n)})));
    }
    if(request.headers.get("authorization")!=="Bearer offline-service-token") return Response.json({ok:false,error:"unauthorized"},{status:401});
    kvCalls++;
    const body=await request.json();
    if(url.pathname==="/kv/get") return Response.json({ok:true,entry:get(body.key)});
    if(url.pathname==="/kv/getMany") return Response.json({ok:true,entries:body.keys.map(get)});
    if(url.pathname==="/kv/set") {version++;writes++;set(body.key,body.value,body.expireInMs);return Response.json({ok:true,versionstamp:String(version)});}
    if(url.pathname==="/kv/delete") {writes++;entries.delete(JSON.stringify(body.key));return Response.json({ok:true});}
    if(url.pathname==="/kv/atomic") {
      if(body.checks.some(check=>Object.keys(check).some(key=>key!=="key"&&key!=="versionstamp")))return Response.json({ok:false,error:"invalid_atomic_check"},{status:400});
      if(body.checks.some(check=>(get(check.key)?.versionstamp??null)!==check.versionstamp))return Response.json({ok:false});
      version++;writes++;
      for(const m of body.mutations)if(m.type==="set")set(m.key,m.value,m.expireInMs);else entries.delete(JSON.stringify(m.key));
      return Response.json({ok:true,versionstamp:String(version)});
    }
    if(url.pathname==="/kv/list") {
      const prefix=body.prefix??[];
      const pattern=(encode(prefix)+"|").replace(/[\\\\%_]/g,c=>"\\\\"+c)+"%";
      if(new TextEncoder().encode(pattern).length>50)return Response.json({ok:false,error:"d1_like_limit"},{status:500});
      const all=[...entries.values()].filter(e=>get(e.key)&&e.key.length>prefix.length&&prefix.every((p,i)=>p===e.key[i])).sort((a,b)=>encode(a.key)<encode(b.key)?-1:encode(a.key)>encode(b.key)?1:0);
      const remaining=all.filter(entry=>!body.cursor||encode(entry.key)>body.cursor);
      const page=remaining.slice(0,body.limit);
      return Response.json({ok:true,entries:page,cursor:remaining.length>page.length?encode(page[page.length-1].key):null});
    }
    return Response.json({ok:false,error:"not_found"},{status:404});
  }
};
`;
