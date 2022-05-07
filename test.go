package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	ops "github.com/stellar/go/protocols/horizon/operations"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"golang.org/x/sync/errgroup"
)

var (
	HorizonTimeout = 60 * time.Second
)

func mainz() {

	/*
	    ts, err := time.Parse(time.RFC3339, "+39121901036-03-29T15:30:22Z")
		if err != nil {
	      fmt.Println(err)
	      return
	    }
	    fmt.Println(ts)
	*/
	clienttest()
}

func clienttest() {
	// Use the default pubnet client
	client := horizonclient.DefaultPublicNetClient

	// Create an account request
	operationsRequest := horizonclient.OperationRequest{Limit: 200, ForClaimableBalance: "0000000071d3336fa6b6cf81fcbeda85a503ccfabc786ab1066594716f3f9551ea4b89ca"}

	// Load the account detail from the network
	operations, err := client.Operations(operationsRequest)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, operation := range operations.Embedded.Records {
		if operation.GetType() == "create_claimable_balance" {
			cb := operation.(ops.CreateClaimableBalance)

			for _, claimant := range cb.Claimants {
				if claimant.Predicate.Type == xdr.ClaimPredicateTypeClaimPredicateBeforeAbsoluteTime {
					absBefore, _ := claimant.Predicate.GetAbsBefore()
					fmt.Printf("Claimant Predicate AbsBefore epoch value: api json incoming %s, parsed epoch = %v, original epoch = %d\n", "+39121901036-03-29T15:30:22Z", absBefore, 1234567890982222222)
				}
			}

		}
	}

}

// monitor ingestion filtering:
//
// go run test.go <filteredHorizonUrl> <unfilteredHorizonUrl> <generatePayments true/false>
// 
// setup tail on unfiltered_horizon.log and filtered_horizon.log files that are created in current run directory
// to see the difference in tx activity being saved into history.
//
// then update the filters on the one horizon instance to see tx activity change, will need access to the admin port
// for this horizon instance, this example uses 4200 as admin port:
//
// account filter update
// curl --location --request PUT 'http://localhost:4200/ingestion/filters/account' \
//      --header 'Content-Type: app' \
//      --data-raw '{ 
//             "whitelist":[
//               "GCSD5KM3YITXGBETUXZBQETKEP4YCVBLKVHNHI5A24TI2EEOFTA7K3KK"
//             ],
//             "enabled":true
//           }'
//
// asset filter update
// curl --location --request PUT 'http://localhost:4200/ingestion/filters/asset' \
//      --header 'Content-Type: app' \
//      --data-raw '{ 
//             "whitelist":[
//               "USDC:GCSD5KM3YITXGBETUXZBQETKEP4YCVBLKVHNHI5A24TI2EEOFTA7K3KK"
//             ],
//             "enabled":true
//           }'
// 
// since testnet can be quiet, can also trigger an account and payment operations emission onto testnet, by setting third
// parameter to true, it will submit tx's to the filteredhorizon url:
// go run test.go <filteredHorizonUrl> <unfilteredHorizonUrl> true
//
// both horizons should be using Testnet network. Then tail tx_generate.log in the current runtime directory to see the activity of new account ids and payment tx's generated
// from them. It will generate payment tx's for FilterDollar asset. You can add those accounts and/or FilterDollar asset to filter rules
// to then see differences between filtered and non-filtered horizons
// 
func main() {

	// https://localhost:8000/
	filteredHorizon := os.Args[1]
	// https://horizon-testnet.stellar.org/
	unfilteredHorizon := os.Args[2]
	generatePayments, err := strconv.ParseBool(os.Args[3])
	if err != nil {
		generatePayments = false
	}

	txLog, err := os.OpenFile("tx_generate.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer txLog.Close()
	txLogger := log.New(txLog, "Tx Generation:\t", log.Ldate|log.Ltime|log.Lshortfile)
	txLogger.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	unfilteredHorizonLog, err := os.OpenFile("unfiltered_horizon.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer unfilteredHorizonLog.Close()
	unfilteredLogger := log.New(unfilteredHorizonLog, "Tx Received:\t", log.Ldate|log.Ltime|log.Lshortfile)
	unfilteredLogger.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	filteredHorizonLog, err := os.OpenFile("filtered_horizon.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer filteredHorizonLog.Close()
	filteredLogger := log.New(filteredHorizonLog, "Tx Received:\t", log.Ldate|log.Ltime|log.Lshortfile)
	filteredLogger.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	if generatePayments {
		streamPayments(filteredHorizon, txLogger)
	}

	// stream out the log of tx activity on filtered horizon instance for chosen account
	go streamTx(filteredHorizon, getPrintHandler(filteredLogger))
	// stream out the log of tx activity on non-filtered horizon instance for chosen account
	streamTx(unfilteredHorizon, getPrintHandler(unfilteredLogger))
}

func streamPayments(horizonUrl string, txLogger *log.Logger) []*keypair.FromAddress {

    client := &horizonclient.Client{
	    HorizonURL:     horizonUrl,
	    HTTP:           http.DefaultClient,
    }

	networkDetails, err := client.Root()
	if err != nil {
		panic(err)
	}
	txLogger.Printf("network passphrase: %s", networkDetails.NetworkPassphrase)

	count := 2
	accs := make([]*keypair.Full, count)
	accountDetails := make([]horizon.Account, count)

	kp := keypair.MustRandom()
	txLogger.Printf("first account: %v", kp.Address())
	txLogger.Printf("funding first account on friendbot...")
	fundingTx, err := client.Fund(kp.Address())

	if horizonclient.IsNotFoundError(err) {
		fundWithRoot(txLogger, client, networkDetails, kp)
	} else if err != nil {
		panic(err)
	}

	txLogger.Printf("first account funded on ledger %d", fundingTx.Ledger)
	time.Sleep(5 * time.Second)
	firstAccountDetail, err := client.AccountDetail(horizonclient.AccountRequest{AccountID: kp.Address()})
	if err != nil {
		panic(err)
	}

	accs[0] = kp
	accountDetails[0] = firstAccountDetail
	assetDetail := txnbuild.CreditAsset{Code: "FilterDollar", Issuer: accs[0].Address()}

	txLogger.Printf("first account, %s, funded %s XLM, has seq num: %s and issues asset: %v", firstAccountDetail.AccountID, firstAccountDetail.Balances[0].Balance, firstAccountDetail.Sequence, assetDetail)
	txLogger.Printf("creating %d accounts...", count)

	group := errgroup.Group{}
	for i := 1; i < count; i++ {
		i := i
		group.Go(func() error {
			accs[i] = keypair.MustRandom()
			tx, err := createAccountTx(networkDetails.NetworkPassphrase, &accountDetails[0], accs[0], accs[i].Address(), "50")
			if err != nil {
				return fmt.Errorf("building tx %d: %w", i, err)
			}
			txHash, _ := tx.HashHex(networkDetails.NetworkPassphrase)

			var ledger = int32(0)

			ledger, err = submitToHorizon(client, tx)

			if err != nil {
				return fmt.Errorf("submitting tx %d: %w", i, err)
			}

			accountDetails[i], err = client.AccountDetail(horizonclient.AccountRequest{AccountID: accs[i].Address()})
			if err != nil {
				panic(err)
			}
			txLogger.Printf("finished create account, %v, tx seq=%d h=%s, went on ledger=%d ", accs[i].Address(), tx.SequenceNumber(), txHash, ledger)

			tx, err = txnbuild.NewTransaction(
				txnbuild.TransactionParams{
					SourceAccount:        &accountDetails[i],
					IncrementSequenceNum: true,
					BaseFee:              txnbuild.MinBaseFee,
					Timebounds:           txnbuild.NewInfiniteTimeout(),
					Operations: []txnbuild.Operation{
						&txnbuild.ChangeTrust{
							Line:  assetDetail.MustToChangeTrustAsset(),
							Limit: "5000",
						},
					},
				},
			)

			if err != nil {
				return fmt.Errorf("building tx %d: %w", i, err)
			}

			var signedTx *txnbuild.Transaction
			if signedTx, err = tx.Sign(network.TestNetworkPassphrase, accs[i]); err != nil {
				return fmt.Errorf("signing tx %d: %w", i, err)
			}

			ledger, err = submitToHorizon(client, signedTx)

			if err != nil {
				return fmt.Errorf("building tx %d: %w", i, err)
			}

			txLogger.Printf("finished trustline for %v on %v, tx seq=%d h=%s, went on ledger=%d ", assetDetail.GetCode(), accs[i].Address(), tx.SequenceNumber(), txHash, ledger)

			return nil
		})
		time.Sleep(100 * time.Millisecond)
	}
	err = group.Wait()
	if err != nil {
		panic(err)
	}

	txLogger.Printf("Payment Loop with accounts %v and %v and asset %v \n\n", accs[0].Address(), accs[1].Address(), assetDetail)

	go makePaymentLoop(txLogger, client, networkDetails, accs, accountDetails, assetDetail)

	fromAddresses := []*keypair.FromAddress{}
	for _, address := range accs {
		fromAddresses = append(fromAddresses, address.FromAddress())
	}
	return fromAddresses
}

func streamTx(url string, printHandler func(tr horizon.Transaction)) {
	transactionRequest := horizonclient.TransactionRequest{}

	ctx := context.Background()

	filteredClient := &horizonclient.Client{
		HorizonURL: url,
		HTTP:       http.DefaultClient,
		AppName:    "filtertest",
	}

	for {
		err := filteredClient.StreamTransactions(ctx, transactionRequest, printHandler)
		if err != nil {
			fmt.Println(err)
		}
		time.Sleep(5 * time.Second)
	}
}

func getPrintHandler(logger *log.Logger) func(horizon.Transaction) {
	return func(tr horizon.Transaction) {
		var result xdr.TransactionEnvelope
		if err := xdr.SafeUnmarshalBase64(tr.EnvelopeXdr, &result); err != nil {
			logger.Printf("Can't log transaction %v", tr.EnvelopeXdr)
			return
		}

        var operations []xdr.Operation
        var txType string
        var sourceAccount string
		tx1, v1Exists := result.GetV1()
		if v1Exists {
			operations = tx1.Tx.Operations
			txType = "V1"
			sourceAccount = tx1.Tx.SourceAccount.Address()
		}
		tx0, v0Exists := result.GetV0()
		if v0Exists {
			operations = tx0.Tx.Operations
			txType = "V0"
			sourceAccount = fmt.Sprintf("%s",tx0.Tx.SourceAccountEd25519)
		}

		for _, operation := range operations {
			logger.Printf("Transaction %v:%v, Operation %v, Source %v\n\n", tr.Hash[len(tr.Hash)-10:], txType, operation.Body.Type.String(), sourceAccount)
		}
	}
}

func fundWithRoot(logger *log.Logger, client *horizonclient.Client, networkDetails horizon.Root, kp *keypair.Full) {
	logger.Printf("friendbot not available for funding, will try root")
	root := keypair.Root(networkDetails.NetworkPassphrase)
	rootAccountDetail, err := client.AccountDetail(horizonclient.AccountRequest{AccountID: root.Address()})
	if err != nil {
		panic(err)
	}
	logger.Printf("using root account %s", rootAccountDetail.GetAccountID())
	tx, err := createAccountTx(networkDetails.NetworkPassphrase, &rootAccountDetail, root, kp.Address(), "10000")
	if err != nil {
		panic(err)
	}
	fundingTx, err := client.SubmitTransaction(tx)
	if err != nil {
		panic(err)
	}
	txHash, _ := tx.HashHex(networkDetails.NetworkPassphrase)
	txXdr, _ := tx.Base64()
	time.Sleep(5 * time.Second)
	logger.Printf("sent funding from root, tx seq, orig hash, resp hash, id, sent xdr, received xdr %d, %s, %s, %s, %s, %s", tx.SequenceNumber(), txHash, fundingTx.Hash, fundingTx.ID, txXdr, fundingTx.EnvelopeXdr)
}

func makePaymentLoop(logger *log.Logger, client *horizonclient.Client, networkDetails horizon.Root, accs []*keypair.Full, accountDetails []horizon.Account, assetDetail txnbuild.CreditAsset) {

	payeeIndex := 1
	payorIndex := 0
	for {
		tx, err := makePaymentTx(networkDetails.NetworkPassphrase, &accountDetails[payorIndex], accs[payorIndex], accs[payeeIndex].Address(), "5", assetDetail)
		if err != nil {
			logger.Printf("error building tx %v", err)
		}
		txHash, _ := tx.HashHex(networkDetails.NetworkPassphrase)

		_, err = submitToHorizon(client, tx)

		logger.Printf("submitted %v payment tx seq=%d hash=%s from payor=%s to payee=%s", assetDetail.GetCode(), tx.SequenceNumber(), txHash[len(txHash)-10:], accs[payorIndex].Address(), accs[payeeIndex].Address())

		if err != nil {
			logger.Printf("error submitting tx %v", err)
		}

		time.Sleep(5 * time.Second)
		temppay := payeeIndex
		payeeIndex = payorIndex
		payorIndex = temppay
	}
}

func submitToHorizon(client *horizonclient.Client, tx *txnbuild.Transaction) (ledger int32, err error) {
	txResp, err := client.SubmitTransaction(tx)
	if err != nil {
		return 0, err
	}
	return txResp.Ledger, nil
}

func submitToCore(coreURL string, client *horizonclient.Client, tx *txnbuild.Transaction, networkDetails *horizon.Root) (ledger int32, err error) {
	q := url.Values{}

	txHash, err := tx.HashHex(networkDetails.NetworkPassphrase)
	if err != nil {
		return 0, fmt.Errorf("tx hash not working: %v", err)
	}

	txXDR, err := tx.Base64()
	if err != nil {
		return 0, fmt.Errorf("tx XDR not working: %v", err)
	}

	q.Set("blob", txXDR)
	u := coreURL + "/tx?" + q.Encode()
	resp, err := http.Get(u)

	if err != nil {
		return 0, err
	}

	coreResp := struct {
		Status string
	}{}
	err = json.NewDecoder(resp.Body).Decode(&coreResp)
	if err != nil {
		return 0, err
	}

	if coreResp.Status != "PENDING" {
		return 0, fmt.Errorf("tx status not pending: %v", coreResp.Status)
	}

	for i := 0; i < 20; i++ {
		tx, err := client.TransactionDetail(txHash)
		if horizonclient.IsNotFoundError(err) {
			time.Sleep(500 * time.Millisecond)
		} else if err != nil {
			return 0, err
		} else {
			return tx.Ledger, nil
		}
	}
	return 0, fmt.Errorf("timed out waiting for response")
}

func createAccountTx(networkPassphrase string, creator *horizon.Account, creatorKp *keypair.Full, account string, amount string) (*txnbuild.Transaction, error) {
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        creator,
		IncrementSequenceNum: true,
		BaseFee:              500,
		Timebounds:           txnbuild.NewInfiniteTimeout(),
		Operations: []txnbuild.Operation{
			&txnbuild.CreateAccount{Destination: account, Amount: amount},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("building tx: %w", err)
	}

	tx, err = tx.Sign(networkPassphrase, creatorKp)
	if err != nil {
		return nil, fmt.Errorf("signing tx: %w", err)
	}

	return tx, nil
}

func makePaymentTx(networkPassphrase string, creator *horizon.Account, creatorKp *keypair.Full, toAccount string, amount string, asset txnbuild.CreditAsset) (*txnbuild.Transaction, error) {
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        creator,
		IncrementSequenceNum: true,
		BaseFee:              500,
		Timebounds:           txnbuild.NewInfiniteTimeout(),
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{Destination: toAccount, Amount: amount, Asset: asset},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("building tx: %w", err)
	}

	tx, err = tx.Sign(networkPassphrase, creatorKp)
	if err != nil {
		return nil, fmt.Errorf("signing tx: %w", err)
	}

	return tx, nil
}
