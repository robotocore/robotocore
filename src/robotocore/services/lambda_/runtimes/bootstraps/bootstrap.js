/**
 * Node.js Lambda bootstrap — reads event from stdin, invokes handler, writes result to stdout.
 *
 * Usage: node bootstrap.js <handler>
 * Where handler is "module.function" (e.g., "index.handler")
 *
 * Protocol:
 *   stdin  -> JSON event
 *   stdout -> JSON result
 *   stderr -> logs
 */

const path = require('path');

async function main() {
    const handlerSpec = process.env._HANDLER || process.argv[2];
    if (!handlerSpec) {
        const err = { errorMessage: 'No handler specified', errorType: 'Runtime.HandlerNotFound' };
        process.stdout.write(JSON.stringify(err));
        process.exit(1);
    }

    const dotIndex = handlerSpec.lastIndexOf('.');
    if (dotIndex < 0) {
        const err = { errorMessage: `Bad handler format: ${handlerSpec}`, errorType: 'Runtime.HandlerNotFound' };
        process.stdout.write(JSON.stringify(err));
        process.exit(1);
    }

    const modulePath = handlerSpec.substring(0, dotIndex);
    const funcName = handlerSpec.substring(dotIndex + 1);

    // Read event from stdin
    let inputData = '';
    for await (const chunk of process.stdin) {
        inputData += chunk;
    }

    let event;
    try {
        event = JSON.parse(inputData);
    } catch (e) {
        event = {};
    }

    // Build context
    const context = {
        functionName: process.env.AWS_LAMBDA_FUNCTION_NAME || 'test-function',
        functionVersion: '$LATEST',
        invokedFunctionArn: `arn:aws:lambda:${process.env.AWS_REGION || 'us-east-1'}:${process.env.AWS_ACCOUNT_ID || '123456789012'}:function:${process.env.AWS_LAMBDA_FUNCTION_NAME || 'test-function'}`,
        memoryLimitInMB: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || '128',
        awsRequestId: require('crypto').randomUUID(),
        logGroupName: `/aws/lambda/${process.env.AWS_LAMBDA_FUNCTION_NAME || 'test-function'}`,
        logStreamName: `${new Date().toISOString().split('T')[0].replace(/-/g, '/')}/$LATEST`,
        getRemainingTimeInMillis: () => {
            const timeout = parseInt(process.env.AWS_LAMBDA_FUNCTION_TIMEOUT || '3', 10);
            return timeout * 1000;
        },
        done: (err, result) => { if (err) throw err; return result; },
        succeed: (result) => result,
        fail: (err) => { throw err; },
    };

    // Load the handler module
    let handlerModule;
    try {
        const fullPath = path.resolve(process.cwd(), modulePath);
        handlerModule = require(fullPath);
    } catch (e) {
        const err = {
            errorMessage: `Cannot find module '${modulePath}': ${e.message}`,
            errorType: 'Runtime.ImportModuleError',
        };
        process.stdout.write(JSON.stringify(err));
        process.exit(1);
    }

    const handlerFunc = handlerModule[funcName];
    if (typeof handlerFunc !== 'function') {
        const err = {
            errorMessage: `Handler '${funcName}' is not a function in '${modulePath}'`,
            errorType: 'Runtime.HandlerNotFound',
        };
        process.stdout.write(JSON.stringify(err));
        process.exit(1);
    }

    // Invoke the handler
    try {
        let result;
        if (handlerFunc.length <= 2) {
            // async handler or 2-arg handler
            result = await handlerFunc(event, context);
        } else {
            // callback-style handler(event, context, callback)
            result = await new Promise((resolve, reject) => {
                handlerFunc(event, context, (err, res) => {
                    if (err) reject(err);
                    else resolve(res);
                });
            });
        }
        process.stdout.write(JSON.stringify(result === undefined ? null : result));
    } catch (e) {
        const err = {
            errorMessage: e.message || String(e),
            errorType: e.constructor ? e.constructor.name : 'Error',
            stackTrace: (e.stack || '').split('\n'),
        };
        process.stdout.write(JSON.stringify(err));
        process.exit(1);
    }
}

main().catch(e => {
    process.stderr.write(`Bootstrap error: ${e.message}\n`);
    process.exit(1);
});
